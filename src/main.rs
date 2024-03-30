mod packet;
mod qos;

use core::time;
use log::{debug, error, info, warn};
use std::{
    collections::HashMap,
    io::{Read, Write},
    net::TcpStream,
    sync::{Arc, Mutex},
};

use clap::{arg, Command};

use crate::{packet::Packet, qos::QoS};

fn cli() -> Command {
    Command::new("mqtt-client")
        .arg(arg!(--tmpdir <TMPDIR> "Temporary directory").default_value("/var/tmp/rust-mqtt"))
        .arg(arg!(--broker <BROKER> "Broker address. (HOST:PORT)").default_value("localhost:1883"))
        .arg(arg!(-u --username <USERNAME> "Username"))
        .arg(arg!(-p --password <PASSWORD> "Password").requires("username"))
        .arg(arg!(--clientid <CLIENT_ID> "Client ID"))
        .arg(arg!(--qos <QOS> "QoS. (0, 1, 2)").default_value("0"))
        .arg(arg!(--keepalive <KEEP_ALIVE> "Keep alive (seconds)").default_value("60"))
        .arg(arg!(--cleansession "Clean session"))
        .arg(arg!(--will "Will flag"))
        .arg(arg!(--willtopic <WILL_TOPIC> "Will topic").requires("will"))
        .arg(arg!(--willmessage <WILL_MESSAGE> "Will message").requires("will"))
        .subcommand_required(true)
        .subcommand(
            Command::new("pub")
                .arg(arg!(-t --topic <TOPIC>).required(true))
                .arg(arg!(-m --message <MESSAGE>).required(true)),
        )
        .subcommand(Command::new("sub").arg(arg!(-t --topic <TOPIC>).required(true)))
}

fn connect(address: &str) -> TcpStream {
    TcpStream::connect(address).unwrap()
}

fn consume_published_packet(packet: &packet::PublishPacket) {
    let message = String::from_utf8(packet.payload.clone()).unwrap();
    println!("Received message={}", message);
}

fn main() {
    env_logger::init();

    let matches = cli().get_matches();

    let broker = matches.get_one::<String>("broker").unwrap();
    let username = matches.get_one::<String>("username").cloned();
    let password = matches.get_one::<String>("password").cloned();
    let client_id = matches.get_one::<String>("clientid").cloned();
    let qos: QoS = matches
        .get_one::<String>("qos")
        .unwrap()
        .parse::<u8>()
        .unwrap()
        .into();
    let keep_alive = matches
        .get_one::<String>("keepalive")
        .unwrap()
        .parse::<u16>()
        .unwrap();
    let clean_session = matches.get_flag("cleansession");
    let will_flag = matches.get_flag("will");
    let will_topic = matches.get_one::<String>("willtopic").cloned();
    let will_message = matches.get_one::<String>("willmessage").cloned();

    let mut stream = connect(&broker);
    let mut buffer = [0; 1024];
    let mut unpuback_packets: HashMap<u16, packet::PublishPacket> = HashMap::new();
    let mut unpubrec_packets: HashMap<u16, packet::PublishPacket> = HashMap::new();
    let mut unpubrel_packets: HashMap<u16, packet::PublishPacket> = HashMap::new();
    let mut unpubcomp_packets: HashMap<u16, packet::PubrelPacket> = HashMap::new();

    let wait_for_exit = Arc::new(Mutex::new(false));

    // CONNECT
    let connect_packet = packet::ConnectPacket::new(
        username,
        password,
        client_id,
        keep_alive,
        clean_session,
        will_flag,
        will_topic,
        will_message,
    );
    debug!("Send connect_packet={:?}", connect_packet);
    stream.write(&connect_packet.serialize()).unwrap();
    stream.flush().unwrap();

    let result = stream.read(&mut buffer).unwrap();
    let (connack_packet, mut i) = packet::ConnackPacket::deserialize(&buffer);
    debug!("Received connack_packet={:?}", connack_packet);

    while result > i {
        let (received_packet, replied_packet, next_i) =
            packet::create_replay_packet_with_received_packet(&buffer, i);
        debug!("Received packet={:?}", received_packet);

        if let packet::PacketType::PUBLISH(publish_packet) = received_packet {
            if publish_packet.qos == QoS::QoS2 {
                unpubrel_packets.insert(publish_packet.packet_id.unwrap(), publish_packet.clone());
            }
        } else if let packet::PacketType::PUBCOMP(pubcomp_packet) = received_packet {
            unpubrel_packets.remove(&pubcomp_packet.packet_id);
        }

        if let Some(replied_packet) = replied_packet {
            debug!("Send packet={:?}", replied_packet);
            stream.write(&replied_packet.serialize()).unwrap();
            stream.flush().unwrap();
        }

        i = next_i;
    }

    match matches.subcommand() {
        Some(("pub", sub_matches)) => {
            let topic = sub_matches.get_one::<String>("topic").unwrap();
            let message = sub_matches.get_one::<String>("message").unwrap();
            info!("Publish message='{}' to topic={}", topic, message,);

            let publish_packet = packet::PublishPacket::new(
                false,
                qos,
                false,
                topic.to_string(),
                None,
                message.as_bytes().to_vec(),
            );
            debug!("Send publish_packet={:?}", publish_packet);
            stream.write(&publish_packet.serialize()).unwrap();
            stream.flush().unwrap();

            match publish_packet.qos {
                QoS::QoS0 => { /* NOP */ }
                QoS::QoS1 => {
                    unpuback_packets.insert(publish_packet.packet_id.unwrap(), publish_packet);

                    stream.read(&mut buffer).unwrap();
                    let (puback_packet, _) = packet::PubackPacket::deserialize(&buffer);
                    debug!("Recieved puback_packet={:?}", puback_packet);
                    unpuback_packets.remove(&puback_packet.packet_id);
                }
                QoS::QoS2 => {
                    unpubrec_packets.insert(publish_packet.packet_id.unwrap(), publish_packet);

                    stream.read(&mut buffer).unwrap();
                    let (pubrec_packet, _) = packet::PubrecPacket::deserialize(&buffer);
                    debug!("Received pubrec_packet={:?}", pubrec_packet);
                    unpubrec_packets.remove(&pubrec_packet.packet_id);

                    let pubrel_packet = packet::PubrelPacket {
                        packet_id: pubrec_packet.packet_id,
                    };
                    debug!("Send pubrel_packet={:?}", pubrel_packet);
                    stream.write(&pubrel_packet.serialize()).unwrap();
                    stream.flush().unwrap();
                    unpubcomp_packets.insert(pubrec_packet.packet_id, pubrel_packet.clone());

                    stream.read(&mut buffer).unwrap();
                    let (pubcomp_packet, _) = packet::PubcompPacket::deserialize(&buffer);
                    debug!("Received pubcomp_packet={:?}", pubcomp_packet);
                    unpubcomp_packets.remove(&pubcomp_packet.packet_id);
                }
            }
        }
        Some(("sub", sub_matches)) => {
            let topic = sub_matches.get_one::<String>("topic").unwrap();
            info!("Subscribe topic={}", topic);

            let subscribe_packet = packet::SubscribePacket {
                packet_id: packet::generate_packet_id(),
                topic_filters: vec![(topic.to_string(), qos)],
            };
            debug!("Send subscribe_packet={:?}", subscribe_packet);
            stream.write(&subscribe_packet.serialize()).unwrap();
            stream.flush().unwrap();

            stream.read(&mut buffer).unwrap();
            let (suback_packet, _i) = packet::SubackPacket::deserialize(&buffer);
            debug!("Received suback_packet={:?}", suback_packet);

            if subscribe_packet.packet_id != suback_packet.packet_id {
                error!(
                    "Failed to subscribe topic. SUBACK Packet ID is not matched. subscribe_packet={:?}, suback_packet={:?}",
                    subscribe_packet, suback_packet
                );
                return;
            }

            // Ctrl + C handler thread
            {
                let mut stream = stream.try_clone().unwrap();
                let topic = topic.clone();
                let wait_for_exit = wait_for_exit.clone();
                ctrlc::set_handler(move || {
                    info!("SIGINT received.");

                    stream
                        .write(
                            &packet::UnsubscribePacket {
                                packet_id: packet::generate_packet_id(),
                                topic_filters: vec![topic.to_string()],
                            }
                            .serialize(),
                        )
                        .unwrap();
                    stream.flush().unwrap();

                    *wait_for_exit.lock().unwrap() = true;
                })
                .expect("Error setting Ctrl-C handler");
            }

            // PINGREQ sender thread
            {
                let mut stream = stream.try_clone().unwrap();
                let duration = time::Duration::from_secs(10);
                std::thread::spawn(move || loop {
                    std::thread::sleep(duration);
                    stream.write(&packet::PingreqPacket {}.serialize()).unwrap();
                    stream.flush().unwrap();
                });
            }

            // Process received packets
            loop {
                stream.read(&mut buffer).unwrap();
                let (received_packet, replied_packet, _next_i) =
                    packet::create_replay_packet_with_received_packet(&buffer, 0);
                debug!("Received packet={:?}", received_packet);

                if let packet::PacketType::PUBLISH(publish_packet) = received_packet {
                    if publish_packet.qos == QoS::QoS2 {
                        unpubrel_packets
                            .insert(publish_packet.packet_id.unwrap(), publish_packet.clone());
                    } else {
                        consume_published_packet(&publish_packet);
                    }
                } else if let packet::PacketType::PUBREL(pubrel_packet) = received_packet {
                    // PUBREL受信時に保持しておいたメッセージを削除する (Method A pattern)
                    match unpubrel_packets.remove(&pubrel_packet.packet_id) {
                        Some(publish_packet) => consume_published_packet(&publish_packet),
                        // PUBREL受信時にはブローカーからは削除されているので、再送処理できない?
                        None => warn!(
                            "Unstored publish packet. packet_id={}",
                            pubrel_packet.packet_id
                        ),
                    }
                    unpubcomp_packets.insert(pubrel_packet.packet_id, pubrel_packet.clone());
                } else if let packet::PacketType::PUBCOMP(pubcomp_packet) = received_packet {
                    unpubrel_packets.remove(&pubcomp_packet.packet_id);
                }

                if *wait_for_exit.lock().unwrap() {
                    break;
                }

                if let Some(replied_packet) = replied_packet {
                    debug!("Send packet={:?}", replied_packet);
                    stream.write(&replied_packet.serialize()).unwrap();
                    stream.flush().unwrap();
                }
            }
        }
        _ => unreachable!(),
    }

    let disconnect_packet = packet::DisconnectPacket {};
    debug!("Send disconnect_packet={:?}", disconnect_packet);
    stream.write(&disconnect_packet.serialize()).unwrap();
    stream.flush().unwrap();

    info!("Exit");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_remaining_length() {
        let mut bytes = vec![0; 4];
        packet::insert_remaining_length(&mut bytes);
        assert_eq!(bytes.len(), 5);
        assert_eq!(bytes[1], 0x03);

        let mut bytes = vec![0; 131];
        packet::insert_remaining_length(&mut bytes);
        assert_eq!(bytes.len(), 133);
        assert_eq!(bytes[1], 0x82);
        assert_eq!(bytes[2], 0x01);
    }

    #[test]
    fn test_extract_remaining_length() {
        let bytes = vec![0x00, 0x02, 0x00, 0x00];
        let (length, i) = packet::extract_remaining_length(&bytes);
        assert_eq!(length, 2);
        assert_eq!(i, 2);

        let mut bytes = vec![0; 133];
        bytes.splice(1..2, vec![0x82, 0x01]);
        let (length, i) = packet::extract_remaining_length(&bytes);
        assert_eq!(length, 130);
        assert_eq!(i, 3);
    }

    #[test]
    fn test_serialize_connect_annonymous_packet() {
        let connect_packet = packet::ConnectPacket {
            client_id: "hello".to_string(),
            username: None,
            password: None,
            qos: QoS::QoS0,
            will_retain: false,
            will_flag: false,
            clean_session: true,
            keep_alive: 60,
            will_topic: None,
            will_message: None,
        };
        let bytes = connect_packet.serialize();
        assert_eq!(
            bytes,
            vec![
                // CONNECT=1, 0000
                0b0001_0000,
                // remaining length
                17,
                // Protocol name length (4 bytes)
                0x00,
                0x04,
                // Protocol name (MQTT)
                0x4d,
                0x51,
                0x54,
                0x54,
                // Protocol level (3.1.1 => 4)
                0x04,
                // Control flag (clean session = 1)
                0b0000_0010,
                // keep alive (60 seconds)
                0x00,
                0x3c,
                // Client ID length
                0x00,
                0x05,
                // Client ID (hello)
                0x68,
                0x65,
                0x6c,
                0x6c,
                0x6f,
            ]
        );
    }

    #[test]
    fn test_serialize_connect_username_password_packet() {
        let connect_packet = packet::ConnectPacket {
            client_id: "hello".to_string(),
            username: Some("AAA".to_string()),
            password: Some("BBB".to_string()),
            qos: QoS::QoS0,
            will_retain: false,
            will_flag: false,
            clean_session: true,
            keep_alive: 60,
            will_topic: None,
            will_message: None,
        };
        let bytes = connect_packet.serialize();
        assert_eq!(
            bytes,
            vec![
                // CONNECT=1, 0000
                0b0001_0000,
                // remaining length
                27,
                // Protocol name length (4 bytes)
                0x00,
                0x04,
                // Protocol name (MQTT)
                0x4d,
                0x51,
                0x54,
                0x54,
                // Protocol level (3.1.1 => 4)
                0x04,
                // Control flag (username, password, clean session = 1)
                0b1100_0010,
                // keep alive (60 seconds)
                0x00,
                0x3c,
                // Client ID length
                0x00,
                0x05,
                // Client ID (hello)
                0x68,
                0x65,
                0x6c,
                0x6c,
                0x6f,
                // Username length
                0x00,
                0x03,
                // Username (AAA)
                0x41,
                0x41,
                0x41,
                // Password length
                0x00,
                0x03,
                // Password (BBB)
                0x42,
                0x42,
                0x42,
            ]
        );
    }

    #[test]
    fn test_serialize_connect_will_packet() {
        let connect_packet = packet::ConnectPacket {
            client_id: "hello".to_string(),
            username: None,
            password: None,
            qos: QoS::QoS0,
            will_retain: false,
            will_flag: true,
            clean_session: true,
            keep_alive: 60,
            will_topic: Some("a/b".to_string()),
            will_message: Some("hello".to_string()),
        };
        let bytes = connect_packet.serialize();
        assert_eq!(
            bytes,
            vec![
                // CONNECT=1, 0000
                0b0001_0000,
                // remaining length
                29,
                // Protocol name length (4 bytes)
                0x00,
                0x04,
                // Protocol name (MQTT)
                0x4d,
                0x51,
                0x54,
                0x54,
                // Protocol level (3.1.1 => 4)
                0x04,
                // Control flag (will flag, clean session = 1)
                0b0000_0110,
                // keep alive (60 seconds)
                0x00,
                0x3c,
                // Client ID length
                0x00,
                0x05,
                // Client ID (hello)
                0x68,
                0x65,
                0x6c,
                0x6c,
                0x6f,
                // will topic length
                0x00,
                0x03,
                // will topic (a/b)
                0x61,
                0x2f,
                0x62,
                // will message length
                0x00,
                0x05,
                // will message (hello)
                0x68,
                0x65,
                0x6c,
                0x6c,
                0x6f,
            ]
        );
    }

    #[test]
    fn test_serialize_publish_packet() {
        let publish_packet = packet::PublishPacket {
            dup: false,
            qos: QoS::QoS0,
            retain: false,
            topic_name: "a/b".to_string(),
            packet_id: None,
            payload: "hello".as_bytes().to_vec(),
        };
        let bytes = publish_packet.serialize();
        assert_eq!(
            bytes,
            vec![
                // PUBLISH=3, DUP=0, QoS=0, RETAIN=0
                0b0011_0000,
                // remaining length
                0x0a,
                // topic name length
                0x00,
                0x03,
                // topic name (a/b)
                0x61,
                0x2f,
                0x62,
                // payload (hello)
                0x68,
                0x65,
                0x6c,
                0x6c,
                0x6f,
            ]
        );
    }
}
