#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum QoS {
    QoS0 = 0, // At most once
    QoS1 = 1, // At least once
    QoS2 = 2, // Exactly once
}

impl From<u8> for QoS {
    fn from(v: u8) -> Self {
        match v {
            0 => QoS::QoS0,
            1 => QoS::QoS1,
            2 => QoS::QoS2,
            _ => unreachable!(),
        }
    }
}
