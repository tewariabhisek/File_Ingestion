"""
Synthetic Event Generation Script

This script generates synthetic event data using the Faker library. The events
have predefined domains and event types, and a specified percentage of the events
are duplicates to simulate real-world scenarios.

The script includes:
1. Configurations for domains and event types.
2. A function to generate events with duplicate handling.

Libraries required:
- `faker`: For generating random but realistic-looking data.

Functions:
- generate_events: Generates a list of synthetic events with configurable
  number of events and duplicate ratio.

Usage:
- Run the script directly to output generated events to a specified location,
  or integrate it with a data pipeline.

"""
import os
import json
from faker import Faker
from collections import defaultdict
from uuid import uuid4

fake = Faker()

# Configurations for domains and event types
domains = {
    "account": ["status-change", "profile-update"],
    "transaction": ["payment", "refund"]
}

#Generate Sample Events
"""
    Generates a list of synthetic events with a specified number of events and a
    ratio of duplicate events to unique events and then writes to a json file.
    
    Parameters:
        num_events (int): Total number of events to generate.
        duplicate_ratio (float): Fraction of events that should be duplicates.
    
    Returns:
        list: None
"""
def generate_events(num_events=100, duplicate_ratio=0.1):
    events = []
    duplicate_count = int(num_events * duplicate_ratio)
    unique_event_ids = set()
    
    for _ in range(num_events - duplicate_count):
        domain = fake.random_element(list(domains.keys()))
        event_type = fake.random_element(domains[domain])
        event_id = str(uuid4())
        unique_event_ids.add(event_id)
        
        event = {
            "event_id": event_id,
            "timestamp": fake.iso8601(),
            "domain": domain,
            "event_type": event_type,
            "data": {
                "id": fake.random_int(1000, 9999),
                "old_status": fake.random_element(["ACTIVE", "SUSPENDED"]),
                "new_status": fake.random_element(["ACTIVE", "SUSPENDED"]),
                "reason": fake.sentence()
            }
        }
        events.append(event)
    
    # Add duplicates
    for _ in range(duplicate_count):
        event_id = fake.random_element(list(unique_event_ids))
        duplicate_event = next(event for event in events if event["event_id"] == event_id)
        events.append(duplicate_event)

    # Save to file
    with open("api_output.json", "w") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")

if __name__ == "__main__":
  generate_events()
