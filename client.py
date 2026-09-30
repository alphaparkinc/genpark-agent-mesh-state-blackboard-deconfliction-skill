"""
Multi-Agent Mesh State Blackboard & Conflict Deconfliction Engine (Zero External Dependencies)
Provides optimistic concurrency control, vector clocks, lease-based locking, and conflict resolution.
"""
import time
import math
import hashlib
import json
from typing import Dict, Any, List, Optional

class AgentMeshStateBlackboardDeconflictionEngine:
    def __init__(self, default_lease_seconds: float = 30.0):
        self.default_lease = default_lease_seconds
        self.blackboard: Dict[str, Dict[str, Any]] = {} # key -> {value, version, updated_by, timestamp, vector_clock}
        self.locks: Dict[str, Dict[str, Any]] = {} # resource_id -> {held_by, expires_at, acquired_at}
        self.vector_clock: Dict[str, int] = {} # agent_id -> clock counter
        self.mutation_history: List[Dict[str, Any]] = []

    def propose_state_mutation(
        self,
        agent_id: str,
        state_key: str,
        proposed_value: Any,
        expected_version: Optional[int] = None,
        arbitration_strategy: str = "LAST_WRITE_WINS_WITH_VECTOR_CLOCK"
    ) -> Dict[str, Any]:
        """
        Proposes a mutation to a shared blackboard variable.
        Detects concurrent race conditions using optimistic concurrency versioning.
        """
        now = time.time()
        # Increment agent's vector clock
        self.vector_clock[agent_id] = self.vector_clock.get(agent_id, 0) + 1

        current_entry = self.blackboard.get(state_key)
        current_version = current_entry["version"] if current_entry else 0

        # Check optimistic concurrency
        if expected_version is not None and expected_version != current_version:
            # Race condition conflict detected!
            conflict_details = {
                "state_key": state_key,
                "conflict": "OPTIMISTIC_CONCURRENCY_RACE_DETECTED",
                "expected_version": expected_version,
                "current_version": current_version,
                "current_value": current_entry["value"] if current_entry else None,
                "held_by_last": current_entry["updated_by"] if current_entry else None
            }

            if arbitration_strategy == "REJECT_ON_CONFLICT":
                return {
                    "accepted": False,
                    "resolution": "MUTATION_REJECTED",
                    "conflict_details": conflict_details
                }
            # Else fallback to vector clock or merge

        # Accept mutation
        new_version = current_version + 1
        entry = {
            "state_key": state_key,
            "value": proposed_value,
            "version": new_version,
            "updated_by": agent_id,
            "timestamp": now,
            "vector_clock": dict(self.vector_clock)
        }
        self.blackboard[state_key] = entry

        history_event = {
            "event_index": len(self.mutation_history),
            "state_key": state_key,
            "agent_id": agent_id,
            "version": new_version,
            "timestamp": now
        }
        self.mutation_history.append(history_event)

        return {
            "accepted": True,
            "state_key": state_key,
            "new_version": new_version,
            "updated_by": agent_id,
            "timestamp": now
        }

    def acquire_resource_lock(
        self,
        agent_id: str,
        lock_resource_id: str,
        lease_seconds: Optional[float] = None
    ) -> Dict[str, Any]:
        """Acquires a lease-based mutual exclusion lock on a critical resource."""
        now = time.time()
        lease = lease_seconds or self.default_lease

        current_lock = self.locks.get(lock_resource_id)
        if current_lock:
            if now < current_lock["expires_at"]:
                if current_lock["held_by"] == agent_id:
                    # Renew lock
                    current_lock["expires_at"] = now + lease
                    return {"locked": True, "action": "LOCK_RENEWED", "resource_id": lock_resource_id, "expires_at": current_lock["expires_at"]}
                else:
                    return {
                        "locked": False,
                        "action": "LOCK_CONTENTION_DENIED",
                        "resource_id": lock_resource_id,
                        "held_by": current_lock["held_by"],
                        "remaining_seconds": round(current_lock["expires_at"] - now, 2)
                    }

        # Grant lock
        self.locks[lock_resource_id] = {
            "resource_id": lock_resource_id,
            "held_by": agent_id,
            "acquired_at": now,
            "expires_at": now + lease
        }
        return {
            "locked": True,
            "action": "LOCK_GRANTED",
            "resource_id": lock_resource_id,
            "held_by": agent_id,
            "expires_at": now + lease
        }

    def release_resource_lock(
        self,
        agent_id: str,
        lock_resource_id: str
    ) -> Dict[str, Any]:
        """Releases an actively held resource lock."""
        current_lock = self.locks.get(lock_resource_id)
        if not current_lock:
            return {"released": True, "reason": "No lock held"}

        if current_lock["held_by"] != agent_id and time.time() < current_lock["expires_at"]:
            return {"released": False, "error": f"Cannot release lock held by {current_lock['held_by']}"}

        del self.locks[lock_resource_id]
        return {"released": True, "resource_id": lock_resource_id}

    def read_shared_state(self, state_key: str) -> Dict[str, Any]:
        """Reads a shared blackboard state variable along with its version and metadata."""
        if state_key not in self.blackboard:
            return {"exists": False, "state_key": state_key, "value": None}
        return {"exists": True, "entry": self.blackboard[state_key]}

    def get_blackboard_telemetry(self) -> Dict[str, Any]:
        now = time.time()
        active_locks = {
            k: v for k, v in self.locks.items() if v["expires_at"] > now
        }
        return {
            "total_variables": len(self.blackboard),
            "total_mutations": len(self.mutation_history),
            "active_locks_count": len(active_locks),
            "participating_agents": list(self.vector_clock.keys()),
            "vector_clock_state": self.vector_clock
        }
