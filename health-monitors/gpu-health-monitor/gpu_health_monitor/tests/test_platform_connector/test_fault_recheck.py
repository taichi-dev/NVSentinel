# Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A requested recheck must observe the fault again, not replay a cached event."""

import json
import time
from threading import Event
from types import SimpleNamespace

import pytest

from gpu_health_monitor.dcgm_watcher import dcgm
from gpu_health_monitor.dcgm_watcher.types import ErrorDetails, HealthDetails, HealthStatus
from gpu_health_monitor.platform_connector.platform_connector import (
    PlatformConnectorConfig,
    PlatformConnectorEventProcessor,
)
from gpu_health_monitor.protos import health_event_pb2 as pb

CODE = "DCGM_FR_VOLATILE_DBE_DETECTED"
WATCH = "DCGM_HEALTH_WATCH_MEM"


@pytest.fixture
def sram_poll(monkeypatch):
    # DCGM wire field IDs for one SRAM UCE, no DRAM UCE or pending remapping,
    # and GPU_RECOVERY_ACTION_NONE (0).
    field_ids = {
        "GET_GPU_RECOVERY_ACTION": 1523,
        "ECC_DBE_VOL_TOTAL": 311,
        "ECC_DBE_VOL_SRM": 343,
        "ECC_DBE_VOL_DEV": 319,
        "THRESHOLD_SRM": 346,
        "ROW_REMAP_PENDING": 396,
        "ROW_REMAP_FAILURE": 395,
    }
    for name, field_id in field_ids.items():
        monkeypatch.setattr(dcgm.dcgm_fields, "DCGM_FI_DEV_" + name, field_id, raising=False)
    monkeypatch.setattr(dcgm.dcgm_structs, "DCGM_FV_FLAG_LIVE_DATA", 1, raising=False)
    samples = [
        SimpleNamespace(
            version=0,
            entityGroupId=1,
            entityId=0,
            fieldId=field_id,
            fieldType=ord("i"),
            status=0,
            unused=0,
            ts=1,
            value=SimpleNamespace(i64=value),
        )
        for field_id, value in [(1523, 0), (311, 1), (343, 1), (319, 0), (346, 0), (396, 0), (395, 0)]
    ]

    def read(handle, entities, fields, flags):
        assert handle == "dcgm-handle"
        assert [(e.entityGroupId, e.entityId) for e in entities] == [(1, 0)]
        assert set(fields) == {1523, 311, 343, 319, 346, 396, 395}
        assert flags == 1  # A cached None must not override a current incident.
        return samples

    monkeypatch.setattr(dcgm.dcgm_agent, "dcgmEntitiesGetLatestValues", read, raising=False)
    incident = SimpleNamespace(
        system=dcgm.dcgm_structs.DCGM_HEALTH_WATCH_MEM,
        health=dcgm.dcgm_structs.DCGM_HEALTH_RESULT_FAIL,
        error=SimpleNamespace(code=4, msg="Detected one volatile double-bit ECC error"),
        entityInfo=SimpleNamespace(entityGroupId=1, entityId=0),
    )
    response = SimpleNamespace(overallHealth=2, incidentCount=1, incidents=[incident])
    group = SimpleNamespace(
        _dcgmHandle=SimpleNamespace(handle="dcgm-handle"),
        health=SimpleNamespace(Check=lambda: response),
    )
    watcher = dcgm.DCGMWatcher(dcgm.types.DCGMWatcherConfig("unused", 10, False), [])
    watcher._error_codes[4] = CODE

    def check():
        details, connected = watcher._perform_health_check(group)
        assert connected
        return {WATCH: details[WATCH]}

    yield check, samples, response
    watcher._callback_thread_pool.shutdown()


def test_sram_counter_without_recovery_action_does_not_request_repair(sram_poll, recheck_processor):
    check, _, _ = sram_poll
    processor, request, sent = recheck_processor
    request_recheck(request)
    processor.health_event_occurred(check(), [0])
    processor.health_event_occurred(check(), [0])
    memory_events = [e for e in sent if e.checkName == "GpuMemWatch"]
    assert memory_events and all(
        e.isHealthy and not e.isFatal and e.recommendedAction == pb.NONE for e in memory_events
    )
    assert processor._recheck.completed is not None


def test_sram_filter_accepts_reordered_fields_and_renamed_recovery_field(sram_poll, monkeypatch):
    check, samples, _ = sram_poll
    samples.reverse()  # DCGM's returned fields need not follow request order.
    monkeypatch.delattr(dcgm.dcgm_fields, "DCGM_FI_DEV_GET_GPU_RECOVERY_ACTION")
    monkeypatch.setattr(dcgm.dcgm_fields, "DCGM_FI_DEV_GPU_RECOVERY_ACTION", 1523, raising=False)
    assert check()[WATCH].entity_failures == {}


def test_sram_counter_clears_prior_fault_and_later_required_reset_is_reported(sram_poll, recheck_processor):
    check, samples, _ = sram_poll
    processor, request, sent = recheck_processor
    samples[0].value.i64 = 1  # Driver requires reset.
    processor.health_event_occurred(check(), [0])
    assert [(e.isHealthy, e.isFatal) for e in sent if e.checkName == "GpuMemWatch"] == [(False, True)]
    samples[0].value.i64 = 0
    request_recheck(request)
    processor.health_event_occurred(check(), [0])
    processor.health_event_occurred(check(), [0])
    assert [(e.isHealthy, e.isFatal) for e in sent if e.checkName == "GpuMemWatch"] == [(False, True), (True, False)]
    samples[0].value.i64 = 1
    processor.health_event_occurred(check(), [0])
    assert [(e.isHealthy, e.isFatal) for e in sent if e.checkName == "GpuMemWatch"] == [
        (False, True),
        (True, False),
        (False, True),
    ]


@pytest.mark.parametrize(
    "index,value", [(0, 1), (0, 2), (0, 3), (0, 4), (0, 99), (1, 2), (2, 0), (3, 1), (4, 1), (5, 1), (6, 1)]
)
def test_required_recovery_or_other_memory_fault_keeps_ecc_failure(sram_poll, index, value):
    check, samples, _ = sram_poll
    samples[index].value.i64 = value
    assert check()[WATCH].entity_failures[0][0].code == CODE


@pytest.mark.parametrize(
    "attribute,value", [("status", -1), ("fieldType", ord("d")), ("ts", 0), ("entityId", 1), ("entityGroupId", 3)]
)
def test_invalid_ecc_telemetry_cannot_clear_fault(sram_poll, attribute, value):
    check, samples, _ = sram_poll
    setattr(samples[0], attribute, value)
    assert check()[WATCH].entity_failures[0][0].code == CODE


@pytest.mark.parametrize("invalid", ["missing", "duplicate", "blank", "negative", "unsupported", "timeout"])
def test_unavailable_ecc_telemetry_cannot_clear_fault(sram_poll, monkeypatch, invalid):
    check, samples, _ = sram_poll
    if invalid == "missing":
        samples.pop()
    elif invalid == "duplicate":
        samples.append(samples[0])
    elif invalid == "blank":
        samples[2].value.i64 = dcgm.dcgmvalue.DCGM_INT64_BLANK
    elif invalid == "negative":
        samples[2].value.i64 = -1
    elif invalid == "unsupported":
        monkeypatch.delattr(dcgm.dcgm_fields, "DCGM_FI_DEV_GET_GPU_RECOVERY_ACTION")
    else:

        def fail(*args):
            raise dcgm.dcgm_structs.DCGMError_Timeout()

        monkeypatch.setattr(dcgm.dcgm_agent, "dcgmEntitiesGetLatestValues", fail)
    assert check()[WATCH].entity_failures[0][0].code == CODE


def test_sram_filter_preserves_other_incidents_on_the_same_gpu(sram_poll):
    check, _, response = sram_poll
    response.incidents.append(
        SimpleNamespace(
            system=dcgm.dcgm_structs.DCGM_HEALTH_WATCH_MEM,
            health=dcgm.dcgm_structs.DCGM_HEALTH_RESULT_FAIL,
            error=SimpleNamespace(code=0, msg="Other memory failure"),
            entityInfo=SimpleNamespace(entityGroupId=1, entityId=0),
        )
    )
    response.incidentCount = 2
    details = check()[WATCH]
    assert details.status == HealthStatus.FAIL
    assert [e.code for e in details.entity_failures[0]] == ["DCGM_FR_UNKNOWN"]


@pytest.fixture
def recheck_processor(tmp_path, monkeypatch):
    for key in list(__import__("os").environ):
        if key.startswith("HEALTH_PUBLISH_"):
            monkeypatch.delenv(key)
    metadata = tmp_path / "gpu.json"
    metadata.write_text(json.dumps({"gpus": [{"gpu_id": 0, "uuid": "GPU-test", "pci_address": "0000:18:00.0"}]}))
    state = tmp_path / "state"
    state.write_text("test-boot")
    request = tmp_path / "request"
    request.write_text("")
    config = PlatformConnectorConfig(
        socket_path="unused",
        node_name="gpu-20",
        dcgm_errors_info_dict={CODE: "COMPONENT_RESET"},
        state_file_path=str(state),
        metadata_path=str(metadata),
        processing_strategy=pb.EXECUTE_REMEDIATION,
        recheck_request_path=str(request),
    )
    processor = PlatformConnectorEventProcessor(config, Event())
    sent = []

    def publish(events, **kwargs):
        sent.extend(pb.HealthEvent.FromString(e.SerializeToString()) for e in events)
        return True

    monkeypatch.setattr(processor, "send_health_event_with_retries", publish)
    yield processor, request, sent
    processor.close()


def poll(processor, failing=True, complete=True, observed_at=None):
    processor.health_event_occurred(
        {
            WATCH: HealthDetails(
                HealthStatus.FAIL if failing else HealthStatus.PASS,
                {0: [ErrorDetails(CODE, "Detected one volatile DBE")]} if failing else {},
                is_complete=complete,
                observed_at=time.monotonic() if observed_at is None else observed_at,
            )
        },
        [0],
    )


def request_recheck(path, request_id="first"):
    path.write_text(json.dumps({"id": request_id, "nodeUID": "node-uid", "errorCodes": [CODE]}))


def test_persistent_fault_is_confirmed_once_after_request(recheck_processor):
    processor, path, sent = recheck_processor
    poll(processor)
    assert len([e for e in sent if not e.isHealthy]) == 1
    request_recheck(path)
    poll(processor)  # Read the request; this poll may have started before it.
    assert len([e for e in sent if not e.isHealthy]) == 1
    poll(processor)
    faults = [e for e in sent if not e.isHealthy]
    assert len(faults) == 2
    assert faults[-1].metadata["gpuRecheckRequest"] == "first"
    assert faults[-1].metadata["gpuRecheckNodeUID"] == "node-uid"
    assert [(e.entityType, e.entityValue) for e in faults[-1].entitiesImpacted] == [
        ("GPU", "0"),
        ("PCI", "0000:18:00.0"),
        ("GPU_UUID", "GPU-test"),
    ]
    assert faults[-1].generatedTimestamp.ToNanoseconds() > faults[0].generatedTimestamp.ToNanoseconds()
    for _ in range(3):
        poll(processor)
    assert len([e for e in sent if not e.isHealthy]) == 2


def test_recovered_fault_is_not_replayed(recheck_processor):
    processor, path, sent = recheck_processor
    poll(processor)
    request_recheck(path)
    poll(processor, failing=False)
    poll(processor, failing=False)
    assert len([e for e in sent if not e.isHealthy]) == 1
    assert sent[-1].isHealthy


def test_incomplete_check_waits_for_complete_observation(recheck_processor):
    processor, path, sent = recheck_processor
    poll(processor)
    request_recheck(path)
    poll(processor)
    poll(processor, complete=False)
    assert len([e for e in sent if not e.isHealthy]) == 1
    poll(processor)
    assert len([e for e in sent if not e.isHealthy]) == 2


def test_failed_delivery_retries_same_request(recheck_processor, monkeypatch):
    processor, path, sent = recheck_processor
    poll(processor)
    request_recheck(path)
    poll(processor)
    publish = processor.send_health_event_with_retries
    monkeypatch.setattr(processor, "send_health_event_with_retries", lambda *args, **kwargs: False)
    poll(processor)
    monkeypatch.setattr(processor, "send_health_event_with_retries", publish)
    poll(processor)
    assert len([e for e in sent if not e.isHealthy]) == 2
    poll(processor)
    assert len([e for e in sent if not e.isHealthy]) == 2


def test_removed_request_cancels_pending_recheck(recheck_processor):
    processor, path, sent = recheck_processor
    poll(processor)
    request_recheck(path)
    poll(processor)
    path.write_text("")
    poll(processor)
    assert len([e for e in sent if not e.isHealthy]) == 1


def test_queued_old_samples_cannot_confirm_fault(recheck_processor):
    processor, path, sent = recheck_processor
    poll(processor)
    queued_at = time.monotonic()
    request_recheck(path)
    poll(processor, observed_at=queued_at)
    poll(processor, observed_at=queued_at)
    poll(processor, observed_at=queued_at)
    assert len([e for e in sent if not e.isHealthy]) == 1
    poll(processor)
    assert len([e for e in sent if not e.isHealthy]) == 2


@pytest.mark.parametrize("raw", ["{}", "{", "[]", "x" * 4097, '{"id":"a","nodeUID":"n","errorCodes":["other"]}'])
def test_invalid_request_does_not_republish(recheck_processor, raw):
    processor, path, sent = recheck_processor
    poll(processor)
    path.write_text(raw)
    poll(processor)
    poll(processor)
    assert len([e for e in sent if not e.isHealthy]) == 1


def test_store_only_is_not_promoted_by_recheck(recheck_processor):
    processor, path, sent = recheck_processor
    processor._processing_strategy = pb.STORE_ONLY
    poll(processor)
    request_recheck(path)
    poll(processor)
    poll(processor)
    assert len([e for e in sent if not e.isHealthy]) == 1
    assert all(e.processingStrategy == pb.STORE_ONLY for e in sent)


def test_healthy_confirmation_leaves_future_fault_detection_enabled(recheck_processor):
    processor, path, sent = recheck_processor
    request_recheck(path)
    poll(processor, failing=False)
    poll(processor, failing=False)
    poll(processor)
    faults = [e for e in sent if not e.isHealthy]
    assert len(faults) == 1
    assert "gpuRecheckRequest" not in faults[0].metadata


def test_missing_timestamp_is_not_fresh_evidence(recheck_processor):
    processor, path, sent = recheck_processor
    poll(processor)
    request_recheck(path)
    poll(processor)
    poll(processor, observed_at=0)
    assert len([e for e in sent if not e.isHealthy]) == 1


def test_partial_gpu_coverage_cannot_complete_request(recheck_processor):
    processor, path, sent = recheck_processor
    poll(processor)
    request_recheck(path)
    poll(processor)
    processor.health_event_occurred(
        {
            WATCH: HealthDetails(
                HealthStatus.FAIL,
                {0: [ErrorDetails(CODE, "DBE")]},
                evaluated_gpu_ids={0},
                observed_at=time.monotonic(),
            )
        },
        [0, 1],
    )
    assert len([e for e in sent if not e.isHealthy]) == 1
    poll(processor)
    assert len([e for e in sent if not e.isHealthy]) == 2


def test_multiple_gpus_confirmed_in_one_request(recheck_processor):
    processor, path, sent = recheck_processor

    def both():
        processor.health_event_occurred(
            {
                WATCH: HealthDetails(
                    HealthStatus.FAIL,
                    {0: [ErrorDetails(CODE, "DBE")], 1: [ErrorDetails(CODE, "DBE")]},
                    observed_at=time.monotonic(),
                )
            },
            [0, 1],
        )

    both()
    request_recheck(path)
    both()
    both()
    both()
    faults = [e for e in sent if not e.isHealthy]
    assert len(faults) == 4
    assert len([e for e in faults if e.metadata.get("gpuRecheckRequest") == "first"]) == 2
