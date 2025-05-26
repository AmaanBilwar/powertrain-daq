## Code todos

- [client.py](testing/client.py)
    - [ ] offline listening of can messages and syncing to db
    - [ ] if online then add [marple upload](testing/marple_test.py) script
    

## board config

- [ ] [client.py](testing/client.py) runs all the time -> systemd service
    - restarts if crash
- [ ] [server.py](testing/server.py) always running (deployment)
- [ ] [marple_testing.py](testing/marple_test.py) -> Cron job [perhaps] -> scheduled syncing 