use anyhow::anyhow;
use chrono::{Datelike, Duration, Months, NaiveDate, NaiveDateTime, NaiveTime};
use substreams_database_change::change::AsString;
use substreams_ethereum::pb::eth::v2 as eth;

#[derive(Debug, PartialEq)]
pub struct BlockTimestamp(chrono::NaiveDateTime);

fn first_day_of_month(input: &str) -> NaiveDate {
    let with_day_01 = format!("{}01", input);
    NaiveDate::parse_from_str(&with_day_01, "%Y%m%d").unwrap()
}

fn last_day_of_month(input: &str) -> NaiveDate {
    let with_day_01 = format!("{}01", input);

    NaiveDate::parse_from_str(&with_day_01, "%Y%m%d")
        .unwrap()
        .pred_opt() // minus one day
        .unwrap()
        .checked_add_months(Months::new(1)) // plus one month
        .unwrap()
}

impl BlockTimestamp {
    pub fn from_key(key: &str) -> Self {
        Self::try_from_key(key).unwrap()
    }

    pub fn try_from_key(key: &str) -> Result<Self, anyhow::Error> {
        let mut parts = key.split(":");

        match parts.next().unwrap() {
            "day" => {
                let first_or_last = parts.next().unwrap();
                let date = NaiveDate::parse_from_str(parts.next().unwrap(), "%Y%m%d").unwrap();
                match first_or_last {
                    "first" => Ok(BlockTimestamp(date.and_hms_opt(0, 0, 0).unwrap())),
                    "last" => Ok(BlockTimestamp(
                        date.and_hms_milli_opt(23, 59, 59, 999).unwrap(),
                    )),
                    _ => Err(anyhow!("invalid key")),
                }
            }
            "month" => match parts.next().unwrap() {
                "first" => Ok(BlockTimestamp(
                    first_day_of_month(parts.next().unwrap())
                        .and_hms_opt(0, 0, 0)
                        .unwrap(),
                )),
                "last" => Ok(BlockTimestamp(
                    last_day_of_month(parts.next().unwrap())
                        .and_hms_milli_opt(23, 59, 59, 999)
                        .unwrap(),
                )),
                _ => Err(anyhow!("invalid key")),
            },
            _ => Err(anyhow!("invalid key")),
        }
    }

    pub fn from_block(blk: &eth::Block) -> Self {
        let header = blk.header.as_ref().unwrap();
        let timestamp = header.timestamp.as_ref().unwrap();

        BlockTimestamp(
            NaiveDateTime::from_timestamp_opt(timestamp.seconds, timestamp.nanos as u32)
                .unwrap_or_else(|| panic!("invalid date for timestamp {}", timestamp)),
        )
    }

    pub fn start_of_day(&self) -> NaiveDateTime {
        self.0.date().and_time(NaiveTime::default())
    }

    pub fn start_of_day_key(&self) -> String {
        self.start_of_day().format("day:first:%Y%m%d").to_string()
    }

    pub fn start_of_month(&self) -> NaiveDateTime {
        self.0
            .with_day(1)
            .unwrap()
            .date()
            .and_time(NaiveTime::default())
    }

    pub fn start_of_month_key(&self) -> String {
        self.start_of_month().format("month:first:%Y%m").to_string()
    }

    pub fn end_of_day(&self) -> NaiveDateTime {
        self.0.date().and_time(last_time())
    }

    pub fn end_of_day_key(&self) -> String {
        self.end_of_day().format("day:last:%Y%m%d").to_string()
    }

    pub fn end_of_month(&self) -> NaiveDateTime {
        // Next month calculation
        let (y, m) = match (self.0.year(), self.0.month()) {
            (y, m) if m == 12 => (y + 1, 1),
            (y, m) => (y, m + 1),
        };
        let start_of_next_month = NaiveDate::from_ymd_opt(y, m, 1)
            .unwrap()
            .and_time(NaiveTime::default());
        let end_of_month = start_of_next_month - Duration::nanoseconds(1);

        end_of_month
    }

    pub fn end_of_month_key(&self) -> String {
        self.end_of_month().format("month:last:%Y%m").to_string()
    }
}

impl AsString for BlockTimestamp {
    fn as_string(self) -> String {
        self.to_string()
    }
}

impl AsString for &BlockTimestamp {
    fn as_string(self) -> String {
        self.to_string()
    }
}

impl ToString for BlockTimestamp {
    fn to_string(&self) -> String {
        self.0.to_string()
    }
}

impl Into<String> for BlockTimestamp {
    fn into(self) -> String {
        self.0.to_string()
    }
}

fn last_time() -> NaiveTime {
    NaiveTime::from_hms_nano_opt(23, 59, 59, 999999999).unwrap()
}

#[cfg(test)]
mod tests {
    use super::BlockTimestamp;
    use chrono::NaiveDate;

    fn timestamp(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        min: u32,
        sec: u32,
        millis: u32,
    ) -> BlockTimestamp {
        BlockTimestamp(
            NaiveDate::from_ymd_opt(year, month, day)
                .unwrap()
                .and_hms_milli_opt(hour, min, sec, millis)
                .unwrap(),
        )
    }

    #[test]
    fn it_block_timestamp_try_from_key() {
        assert_eq!(
            BlockTimestamp::from_key("1435708800000"),
            timestamp(2015, 07, 01, 00, 00, 00, 000)
        );

        assert_eq!(
            BlockTimestamp::from_key("1669852799999"),
            timestamp(2022, 11, 30, 23, 59, 59, 999)
        );
    }

    #[test]
    fn it_block_timestamp_start_of_day() {
        let input = timestamp(2021, 7, 5, 10, 21, 54, 354);
        assert_eq!(input.start_of_day().to_string(), "2021-07-05 00:00:00");
    }

    #[test]
    fn it_block_timestamp_start_of_month() {
        let input = timestamp(2021, 7, 5, 10, 21, 54, 354);
        assert_eq!(input.start_of_month().to_string(), "2021-07-01 00:00:00");
    }

    #[test]
    fn it_block_timestamp_end_of_day() {
        let input = timestamp(2021, 7, 5, 10, 21, 54, 354);
        assert_eq!(
            input.end_of_day().to_string(),
            "2021-07-05 23:59:59.999999999"
        );
    }

    #[test]
    fn it_block_timestamp_end_of_month() {
        let input = timestamp(2021, 7, 5, 10, 21, 54, 354);
        assert_eq!(
            input.end_of_month().to_string(),
            "2021-07-31 23:59:59.999999999"
        );
    }
}