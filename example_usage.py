"""Example usage for AgentMeshStateBlackboardDeconflictionEngine."""
import json
from client import AgentMeshStateBlackboardDeconflictionEngine

def main():
    print("=== Multi-Agent Mesh State Blackboard Deconfliction Demo ===")
    blackboard = AgentMeshStateBlackboardDeconflictionEngine()

    # 1. Flight booking agent acquires lock on resource
    print("\n--- 1. Flight Agent Acquires Mutex Lock on Budget Pool ---")
    lock1 = blackboard.acquire_resource_lock("agent_flight", "vacation_budget_usd", lease_seconds=15.0)
    print(json.dumps(lock1, indent=2))

    # 2. Hotel agent attempts concurrent lock -> contention prevented
    print("\n--- 2. Hotel Agent Attempts Concurrent Lock (Collision Prevented) ---")
    lock2 = blackboard.acquire_resource_lock("agent_hotel", "vacation_budget_usd", lease_seconds=15.0)
    print(f"Lock Granted to Hotel Agent: {lock2['locked']} (Held by: {lock2.get('held_by')})")

    # 3. Flight agent writes remaining budget mutation
    print("\n--- 3. Flight Agent Mutates Blackboard State ---")
    mut1 = blackboard.propose_state_mutation("agent_flight", "remaining_budget", 1450.0, expected_version=0)
    print(json.dumps(mut1, indent=2))

    # 4. Release lock & Hotel agent acquires
    blackboard.release_resource_lock("agent_flight", "vacation_budget_usd")
    lock3 = blackboard.acquire_resource_lock("agent_hotel", "vacation_budget_usd")
    print(f"\nPost-release Hotel Agent Lock Granted: {lock3['locked']}")

if __name__ == "__main__":
    main()
