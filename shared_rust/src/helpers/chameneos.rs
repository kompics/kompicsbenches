pub enum ChameneosColour {
    Red,
    Yellow,
    Blue,
    Faded,
}

impl ChameneosColour {
    pub fn for_id(i: usize) -> ChameneosColour {
        match i % 3usize {
            0 => ChameneosColour::Red,
            1 => ChameneosColour::Yellow,
            2 => ChameneosColour::Blue,
            _ => unreachable!(),
        }
    }

    pub fn complement(&self, other: ChameneosColour) -> ChameneosColour {
        match self {
            ChameneosColour::Red => match other {
                ChameneosColour::Red => ChameneosColour::Red,
                ChameneosColour::Yellow => ChameneosColour::Blue,
                ChameneosColour::Blue => ChameneosColour::Yellow,
                ChameneosColour::Faded => ChameneosColour::Faded,
            },
            ChameneosColour::Yellow => match other {
                ChameneosColour::Red => ChameneosColour::Blue,
                ChameneosColour::Yellow => ChameneosColour::Yellow,
                ChameneosColour::Blue => ChameneosColour::Red,
                ChameneosColour::Faded => ChameneosColour::Faded,
            },
            ChameneosColour::Blue => match other {
                ChameneosColour::Red => ChameneosColour::Yellow,
                ChameneosColour::Yellow => ChameneosColour::Red,
                ChameneosColour::Blue => ChameneosColour::Blue,
                ChameneosColour::Faded => ChameneosColour::Faded,
            },
            ChameneosColour::Faded => ChameneosColour::Faded,
        }
    }
}
