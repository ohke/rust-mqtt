# rust-mqtt
RustでのMQTTクライアントの実装例です。

## 使い方

```
Usage: rust-mqtt [OPTIONS] <COMMAND>

Commands:
  pub   
  sub   
  help  Print this message or the help of the given subcommand(s)

Options:
      --tmpdir <TMPDIR>             Temporary directory [default: /var/tmp/rust-mqtt]
      --broker <BROKER>             Broker address. (HOST:PORT) [default: localhost:1883]
  -u, --username <USERNAME>         Username
  -p, --password <PASSWORD>         Password
      --clientid <CLIENT_ID>        Client ID
      --qos <QOS>                   QoS. (0, 1, 2) [default: 0]
      --keepalive <KEEP_ALIVE>      Keep alive (seconds) [default: 60]
      --cleansession                Clean session
      --will                        Will flag
      --willtopic <WILL_TOPIC>      Will topic
      --willmessage <WILL_MESSAGE>  Will message
  -h, --help                        Print help
```

```bash
# ブローカーの起動
$ docker compose up

# subscribe
$ cargo run -- sub -t test/greeting

# publish
$ cargo run -- pub -t test/greeting -m "Hello."
```

### ユーザーを作成する場合
```bash
$ mosquitto_passwd -c -b ./docker/mqtt-broker/config/password.txt alice alicepass
$ docker compose up

# subscribe
$ cargo run -- -u alice -p alicepass sub -t test/greeting
# publish
$ cargo run -- -u alice -p alicepass pub -t test/greeting -m "Hello."
```
