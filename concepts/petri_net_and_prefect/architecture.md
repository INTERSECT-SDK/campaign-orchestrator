              ┌─────────────────────────────┐
              │ YAML Petri Net Definition   │
              └──────────────┬──────────────┘
                             │
                       [ Petri Loader ]
                             │
                 ┌────────────┴────────────┐
                 │ Prefect Flow Controller │
                 └────────────┬────────────┘
                             │
          ┌──────────────────┼──────────────────┐
          │                  │                  │
   [Transition→AMQP]   [Transition→MQTT]   [Transition→Local]
          │                  │                  │
  → Publish message    → Publish topic     → Run directly
          │                  │
   [Remote Worker(s)]   [Agent(s)]   ←→  [Event Listener]
          │
   → Return “task done” event
          │
   → Update Petri marking (add token)
