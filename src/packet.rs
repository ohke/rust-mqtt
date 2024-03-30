use rand::prelude::*;
use std::{any::Any, fmt::Debug};

use crate::qos::QoS;

pub(crate) trait Packet: Any + Debug + 'static {
    fn serialize(&self) -> Vec<u8>;

    // 2つの返り値は、(デシリアライズしたオブジェクト, デシリアライズしたバッファバイト数)
    fn deserialize(buf: &[u8]) -> (Self, usize)
    where
        Self: Sized;
}

#[derive(Debug)]
pub(crate) enum PacketType {
    CONNECT(ConnectPacket),
    CONNACK(ConnackPacket),
    PUBLISH(PublishPacket),
    PUBACK(PubackPacket),
    PUBREC(PubrecPacket),
    PUBREL(PubrelPacket),
    PUBCOMP(PubcompPacket),
    SUBSCRIBE(SubscribePacket),
    SUBACK(SubackPacket),
    UNSUBSCRIBE(UnsubscribePacket),
    UNSUBACK(UnsubackPacket),
    PINGREQ(PingreqPacket),
    PINGRESP(PingrespPacket),
    DISCONNECT(DisconnectPacket),
}

impl Packet for PacketType {
    fn serialize(&self) -> Vec<u8> {
        match self {
            PacketType::CONNECT(packet) => packet.serialize(),
            PacketType::CONNACK(packet) => packet.serialize(),
            PacketType::PUBLISH(packet) => packet.serialize(),
            PacketType::PUBACK(packet) => packet.serialize(),
            PacketType::PUBREC(packet) => packet.serialize(),
            PacketType::PUBREL(packet) => packet.serialize(),
            PacketType::PUBCOMP(packet) => packet.serialize(),
            PacketType::SUBSCRIBE(packet) => packet.serialize(),
            PacketType::SUBACK(packet) => packet.serialize(),
            PacketType::UNSUBSCRIBE(packet) => packet.serialize(),
            PacketType::UNSUBACK(packet) => packet.serialize(),
            PacketType::PINGREQ(packet) => packet.serialize(),
            PacketType::PINGRESP(packet) => packet.serialize(),
            PacketType::DISCONNECT(packet) => packet.serialize(),
        }
    }

    fn deserialize(buf: &[u8]) -> (Self, usize)
    where
        Self: Sized,
    {
        match buf[0] & 0b1111_0000 {
            0b0001_0000 => {
                let (packet, size) = ConnectPacket::deserialize(buf);
                (PacketType::CONNECT(packet), size)
            }
            0b0010_0000 => {
                let (packet, size) = ConnackPacket::deserialize(buf);
                (PacketType::CONNACK(packet), size)
            }
            0b0011_0000 => {
                let (packet, size) = PublishPacket::deserialize(buf);
                (PacketType::PUBLISH(packet), size)
            }
            0b0100_0000 => {
                let (packet, size) = PubackPacket::deserialize(buf);
                (PacketType::PUBACK(packet), size)
            }
            0b0101_0000 => {
                let (packet, size) = PubrecPacket::deserialize(buf);
                (PacketType::PUBREC(packet), size)
            }
            0b0110_0000 => {
                let (packet, size) = PubrelPacket::deserialize(buf);
                (PacketType::PUBREL(packet), size)
            }
            0b0111_0000 => {
                let (packet, size) = PubcompPacket::deserialize(buf);
                (PacketType::PUBCOMP(packet), size)
            }
            0b1000_0000 => {
                let (packet, size) = SubscribePacket::deserialize(buf);
                (PacketType::SUBSCRIBE(packet), size)
            }
            0b1001_0000 => {
                let (packet, size) = SubackPacket::deserialize(buf);
                (PacketType::SUBACK(packet), size)
            }
            0b1010_0000 => {
                let (packet, size) = UnsubscribePacket::deserialize(buf);
                (PacketType::UNSUBSCRIBE(packet), size)
            }
            0b1011_0000 => {
                let (packet, size) = UnsubackPacket::deserialize(buf);
                (PacketType::UNSUBACK(packet), size)
            }
            0b1100_0000 => {
                let (packet, size) = PingreqPacket::deserialize(buf);
                (PacketType::PINGREQ(packet), size)
            }
            0b1101_0000 => {
                let (packet, size) = PingrespPacket::deserialize(buf);
                (PacketType::PINGRESP(packet), size)
            }
            0b1110_0000 => {
                let (packet, size) = DisconnectPacket::deserialize(buf);
                (PacketType::DISCONNECT(packet), size)
            }
            _ => unimplemented!(),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ConnectPacket {
    pub(crate) client_id: String,
    pub(crate) username: Option<String>,
    pub(crate) password: Option<String>,
    pub(crate) qos: QoS,
    pub(crate) will_retain: bool,
    pub(crate) will_flag: bool,
    pub(crate) clean_session: bool,
    pub(crate) keep_alive: u16,
    pub(crate) will_topic: Option<String>,
    pub(crate) will_message: Option<String>,
}

impl ConnectPacket {
    pub(crate) fn new(
        username: Option<String>,
        password: Option<String>,
        client_id: Option<String>,
        keep_alive: u16,
        clean_session: bool,
        will_flag: bool,
        will_topic: Option<String>,
        will_message: Option<String>,
    ) -> Self {
        let client_id = client_id.clone().unwrap_or_else(|| {
            let mut rng = rand::thread_rng();
            format!("mqttclient{}", rng.gen::<u16>())
        });

        if username.is_none() && password.is_some() {
            panic!("password must not be specified if username is not specified");
        }

        if will_flag && (will_topic.is_none() || will_message.is_none()) {
            panic!("will_topic and will_message must be specified if will_flag is true");
        }

        Self {
            client_id,
            username,
            password,
            qos: QoS::QoS0,
            clean_session,
            keep_alive,
            will_flag,
            will_retain: false,
            will_topic,
            will_message,
        }
    }
}

impl Packet for ConnectPacket {
    fn serialize(&self) -> Vec<u8> {
        let mut bytes = vec![];

        // Fixed header
        bytes.push(0b0001_0000); // CONNECT=1, 0000

        // Variable header
        bytes.extend((4 as u16).to_be_bytes()); // Protocol name length (4 bytes)
        bytes.extend("MQTT".as_bytes()); // Protocol name
        bytes.push(4); // Protocol level (3.1.1 => 4)

        // Control flags
        let control_flag = (self.username.is_some() as u8) << 7
            | (self.password.is_some() as u8) << 6
            | (self.will_retain as u8) << 5
            | (self.qos as u8) << 3
            | (self.will_flag as u8) << 2
            | (self.clean_session as u8) << 1;
        bytes.push(control_flag);

        bytes.extend(self.keep_alive.to_be_bytes()); // Keep alive MSB

        // Payload
        bytes.extend((self.client_id.len() as u16).to_be_bytes()); // Client ID length
        bytes.extend(self.client_id.as_bytes()); // Client ID

        // Will topic, Will message
        if self.will_flag {
            if let Some(will_topic) = &self.will_topic {
                bytes.extend((will_topic.len() as u16).to_be_bytes()); // Will topic length
                bytes.extend(will_topic.as_bytes()); // Will topic
            }

            if let Some(will_message) = &self.will_message {
                bytes.extend((will_message.len() as u16).to_be_bytes()); // Will message length
                bytes.extend(will_message.as_bytes()); // Will message
            }
        }

        // Username, Password
        if let Some(username) = &self.username {
            bytes.extend((username.len() as u16).to_be_bytes()); // Username length
            bytes.extend(username.as_bytes()); // Username

            if let Some(password) = &self.password {
                bytes.extend((password.len() as u16).to_be_bytes()); // Password length
                bytes.extend(password.as_bytes()); // Password
            }
        }

        // Fixed header
        // NOTE: 前もって計算するのが面倒なので、実際のサイズを後から挿入している
        insert_remaining_length(&mut bytes);

        bytes
    }

    fn deserialize(_buf: &[u8]) -> (Self, usize) {
        unimplemented!()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ConnackPacket {
    pub(crate) sp: bool,
    pub(crate) accepted: bool,
    pub(crate) refused_reason: Option<&'static str>,
}

impl Packet for ConnackPacket {
    fn serialize(&self) -> Vec<u8> {
        unimplemented!()
    }

    fn deserialize(buf: &[u8]) -> (Self, usize) {
        // Fixed header
        assert!(buf[0] == 0b00100000);
        assert!(buf[1] == 0b00000010);

        // Variable header
        let sp = buf[2] & 0b00000001 == 1;
        // NOTE: 本来はenumで表現すべきだが、エラーケースの処理はしないので文字列にしておく
        let refused_reason = match buf[3] {
            0 => None,
            1 => Some("unacceptable protocol version"),
            2 => Some("identifier rejected"),
            3 => Some("server unavailable"),
            4 => Some("bad user name or password"),
            5 => Some("not authorized"),
            _ => unreachable!(),
        };

        (
            Self {
                sp,
                accepted: refused_reason.is_none(),
                refused_reason,
            },
            4,
        )
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PublishPacket {
    pub(crate) dup: bool,
    pub(crate) qos: QoS,
    pub(crate) retain: bool,
    pub(crate) topic_name: String,
    pub(crate) packet_id: Option<u16>,
    pub(crate) payload: Vec<u8>,
}

impl PublishPacket {
    pub(crate) fn new(
        dup: bool,
        qos: QoS,
        retain: bool,
        topic_name: String,
        mut packet_id: Option<u16>,
        payload: Vec<u8>,
    ) -> Self {
        if qos == QoS::QoS2 || qos == QoS::QoS1 {
            if packet_id.is_none() {
                packet_id = Some(generate_packet_id());
            }
        }

        Self {
            dup,
            qos,
            retain,
            topic_name,
            packet_id,
            payload,
        }
    }
}

impl Packet for PublishPacket {
    fn serialize(&self) -> Vec<u8> {
        let mut bytes = vec![];

        // Fixed header
        bytes.push(
            (0b0011_0000 as u8)
                | (self.dup as u8) << 3
                | (self.qos as u8) << 1
                | (self.retain as u8),
        );

        // Variable header
        bytes.extend((self.topic_name.len() as u16).to_be_bytes()); // topic length
        bytes.extend(self.topic_name.as_bytes()); // topic name
        if let Some(packet_id) = self.packet_id {
            bytes.extend(packet_id.to_be_bytes()); // packet id
        }

        // Payload
        bytes.extend(self.payload.clone());

        insert_remaining_length(&mut bytes);

        bytes
    }

    fn deserialize(buf: &[u8]) -> (Self, usize) {
        assert!(buf[0] & 0b1111_0000 == 0b0011_0000);
        let dup = buf[0] & 0b0000_1000 == 0b0000_1000;
        let qos: QoS = ((buf[0] & 0b0000_0110) >> 1 as u8).into();
        let retain = buf[0] & 0b0000_0001 == 0b0000_0001;

        let (remaining_length, mut i) = extract_remaining_length(buf);
        let fixed_header_length = i;

        let topic_name_length = u16::from_be_bytes([buf[i], buf[i + 1]]);
        let topic_name =
            String::from_utf8(buf[i + 2..i + 2 + topic_name_length as usize].to_vec()).unwrap();
        i = i + 2 + topic_name_length as usize;

        let packet_id = if qos != QoS::QoS0 {
            i += 2;
            Some(u16::from_be_bytes([buf[i], buf[i + 1]]))
        } else {
            None
        };

        let payload = buf[i..i + (remaining_length - (2 + topic_name_length as usize))].to_vec();

        (
            Self {
                dup,
                qos,
                retain,
                topic_name,
                packet_id,
                payload,
            },
            fixed_header_length + remaining_length,
        )
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PubackPacket {
    pub(crate) packet_id: u16,
}

impl Packet for PubackPacket {
    fn serialize(&self) -> Vec<u8> {
        let mut bytes = vec![];

        // Fixed header
        bytes.push(0b01000000); // PUBACK=4
        bytes.push(0b00000010); // remaining length

        // Variable header
        bytes.extend(self.packet_id.to_be_bytes());

        bytes
    }

    fn deserialize(buf: &[u8]) -> (Self, usize) {
        // Fixed header
        assert!(buf[0] == 0b01000000);
        assert!(buf[1] == 0b00000010);

        // Variable header
        let packet_id = u16::from_be_bytes([buf[2], buf[3]]);

        (Self { packet_id }, 4)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PubrecPacket {
    pub(crate) packet_id: u16, // PUBLISHと同じ
}

impl Packet for PubrecPacket {
    fn serialize(&self) -> Vec<u8> {
        let mut bytes = vec![];

        // Fixed header
        bytes.push(0b0101_0000); // PUBREC=5
        bytes.push(2); // remaining length

        // Variable header
        bytes.extend(self.packet_id.to_be_bytes());

        bytes
    }

    fn deserialize(buf: &[u8]) -> (Self, usize) {
        assert!(buf[0] == 0b0101_0000);
        assert!(buf[1] == 2);

        let packet_id = u16::from_be_bytes([buf[2], buf[3]]);

        (Self { packet_id }, 4)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PubrelPacket {
    pub(crate) packet_id: u16, // PUBRECと同じ
}

impl Packet for PubrelPacket {
    fn serialize(&self) -> Vec<u8> {
        let mut bytes = vec![];

        // Fixed header
        bytes.push(0b0110_0010); // PUBREL=6
        bytes.push(2); // remaining length

        // Variable header
        bytes.extend(self.packet_id.to_be_bytes());

        bytes
    }

    fn deserialize(buf: &[u8]) -> (Self, usize) {
        assert!(buf[0] == 0b0110_0010);
        assert!(buf[1] == 2);

        let packet_id = u16::from_be_bytes([buf[2], buf[3]]);

        (Self { packet_id }, 4)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PubcompPacket {
    pub(crate) packet_id: u16, // PUBRELと同じ
}

impl Packet for PubcompPacket {
    fn serialize(&self) -> Vec<u8> {
        let mut bytes = vec![];

        // Fixed header
        bytes.push(0b0111_0000); // PUBCOMP=7
        bytes.push(2); // remaining length

        // Variable header
        bytes.extend(self.packet_id.to_be_bytes());

        bytes
    }

    fn deserialize(buf: &[u8]) -> (Self, usize) {
        assert!(buf[0] == 0b0111_0000);
        assert!(buf[1] == 2);

        let packet_id = u16::from_be_bytes([buf[2], buf[3]]);

        (Self { packet_id }, 4)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SubscribePacket {
    pub(crate) packet_id: u16,
    pub(crate) topic_filters: Vec<(String, QoS)>,
}

impl Packet for SubscribePacket {
    fn serialize(&self) -> Vec<u8> {
        let mut bytes = vec![];

        // Fixed header
        bytes.push(0b1000_0010); // SUBSCRIBE=8

        // Variable header
        bytes.extend(self.packet_id.to_be_bytes());

        // Payload
        for (topic_name, qos) in &self.topic_filters {
            bytes.extend((topic_name.len() as u16).to_be_bytes()); // topic name length
            bytes.extend(topic_name.as_bytes()); // topic name
            bytes.push(*qos as u8); // Requested QoS
        }

        insert_remaining_length(&mut bytes);

        bytes
    }

    fn deserialize(_buf: &[u8]) -> (Self, usize) {
        unimplemented!()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SubackPacket {
    pub(crate) packet_id: u16,
    pub(crate) maximum_qos: Option<QoS>,
    pub(crate) failure: bool,
}

impl Packet for SubackPacket {
    fn serialize(&self) -> Vec<u8> {
        unimplemented!()
    }

    fn deserialize(buf: &[u8]) -> (Self, usize) {
        assert!(buf[0] == 0b10010000);
        assert!(buf[1] == 3);

        let packet_id = u16::from_be_bytes([buf[2], buf[3]]);

        let (maximum_qos, failure) = match buf[4] {
            0 => (Some(QoS::QoS0), false),
            1 => (Some(QoS::QoS1), false),
            2 => (Some(QoS::QoS2), false),
            128 => (None, true),
            _ => unreachable!(),
        };

        (
            Self {
                packet_id,
                maximum_qos,
                failure,
            },
            5,
        )
    }
}

#[derive(Clone, Debug)]
pub(crate) struct UnsubscribePacket {
    pub(crate) packet_id: u16,
    pub(crate) topic_filters: Vec<String>,
}

impl Packet for UnsubscribePacket {
    fn serialize(&self) -> Vec<u8> {
        let mut bytes = vec![];

        // Fixed header
        bytes.push(0b1010_0010); // UNSUBSCRIBE=10

        // Variable header
        bytes.extend(self.packet_id.to_be_bytes());

        // Payload
        for topic in &self.topic_filters {
            bytes.extend((topic.len() as u16).to_be_bytes());
            bytes.extend(topic.as_bytes());
        }

        insert_remaining_length(&mut bytes);

        bytes
    }

    fn deserialize(buf: &[u8]) -> (Self, usize)
    where
        Self: Sized,
    {
        unimplemented!()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct UnsubackPacket {
    pub(crate) packet_id: u16,
}

impl Packet for UnsubackPacket {
    fn serialize(&self) -> Vec<u8> {
        unimplemented!()
    }

    fn deserialize(buf: &[u8]) -> (Self, usize)
    where
        Self: Sized,
    {
        assert!(buf[0] == 0b1011_0000);
        assert!(buf[1] == 2);

        (
            Self {
                packet_id: u16::from_be_bytes([buf[2], buf[3]]),
            },
            4,
        )
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PingreqPacket {}

impl Packet for PingreqPacket {
    fn serialize(&self) -> Vec<u8> {
        let mut bytes = vec![];

        // Fixed header
        bytes.push(0b1100_0000); // PINGREQ=12
        bytes.push(0); // remaining length

        bytes
    }

    fn deserialize(_buf: &[u8]) -> (Self, usize)
    where
        Self: Sized,
    {
        unimplemented!()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PingrespPacket {}

impl Packet for PingrespPacket {
    fn serialize(&self) -> Vec<u8> {
        todo!()
    }

    fn deserialize(buf: &[u8]) -> (Self, usize)
    where
        Self: Sized,
    {
        assert!(buf[0] == 0b1101_0000);
        assert!(buf[1] == 0);

        (Self {}, 2)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DisconnectPacket {}

impl Packet for DisconnectPacket {
    fn serialize(&self) -> Vec<u8> {
        let mut bytes = vec![];

        // Fixed header
        bytes.push(0b1110_0000); // DISCONNECT=14
        bytes.push(0); // remaining length

        bytes
    }

    fn deserialize(_buf: &[u8]) -> (Self, usize)
    where
        Self: Sized,
    {
        unimplemented!()
    }
}

pub(crate) fn insert_remaining_length(bytes: &mut Vec<u8>) {
    let mut length = bytes.len() - 1;
    if length > 268435455 {
        panic!("Too large packet.");
    }

    let mut length_bytes = vec![];
    loop {
        let b = (length & 0x7f) as u8;
        length >>= 7;
        if length > 0 {
            length_bytes.push(b | 0x80);
        } else {
            length_bytes.push(b);
            break;
        }
    }

    bytes.splice(1..1, length_bytes);
}

pub(crate) fn extract_remaining_length(bytes: &[u8]) -> (usize, usize) {
    // (remaining length, variable header start index)
    let mut length = 0;
    let mut multiplier = 1;
    let mut i = 1;

    // 0x80以上であれば、次のバイトもデコードする
    loop {
        length += ((bytes[i] & 0x7f) as usize) * multiplier;
        multiplier *= 128;
        if (bytes[i] & 0x80) == 0 {
            break;
        }
        i += 1;
    }

    (length, i + 1)
}

pub(crate) fn generate_packet_id() -> u16 {
    let mut rng = rand::thread_rng();
    rng.gen()
}

pub(crate) fn create_replay_packet_with_received_packet(
    buf: &[u8],
    i: usize,
) -> (PacketType, Option<PacketType>, usize) {
    let (packet, size) = PacketType::deserialize(&buf[i..]);
    let replied_packet = match &packet {
        PacketType::PUBLISH(publish_packet) => {
            if publish_packet.qos == QoS::QoS1 {
                Some(PacketType::PUBACK(PubackPacket {
                    packet_id: publish_packet.packet_id.unwrap(),
                }))
            } else if publish_packet.qos == QoS::QoS2 {
                Some(PacketType::PUBREC(PubrecPacket {
                    packet_id: publish_packet.packet_id.unwrap(),
                }))
            } else {
                None
            }
        }
        PacketType::PUBREL(pubrel_packet) => Some(PacketType::PUBCOMP(PubcompPacket {
            packet_id: pubrel_packet.packet_id,
        })),
        PacketType::UNSUBACK(_) => None,
        PacketType::PINGRESP(_) => None,
        _ => unimplemented!(),
    };

    // (受信したパケット、返信すべきパケット、次に読み出すインデックス)
    (packet, replied_packet, i + size)
}
