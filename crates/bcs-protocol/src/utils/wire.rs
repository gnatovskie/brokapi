//! Wire forms the live API needs, where they differ from what serde gives by default.
//!
//! * [`command_number`]: a price of an order command goes out as given. If it has more than 8
//!   decimals, its trailing zeros are dropped. The docs allow up to 8 decimals. The live API refused
//!   `"price":279.7600000000` with `INVALID_FORMAT` and took `279.71` (05.11.2026). A [`Decimal`]
//!   keeps the scale of the arithmetic that made it, so a price rounded to the step can carry ten
//!   trailing zeros. `244.30` stays `244.30`: only the zeros past the limit go.
//! * [`one_or_many`]: a field the docs describe as a list, which the live API sends as one object.
//!   `GET /orders` answered `"data": {...}` instead of `"data": [{...}]` (05.11.2026).
//! * [`int_or_text`]: a code the docs describe as an integer, which the live API sends as text.
//!   `GET /orders` answered `"timeInForce":"1"`, the docs say `1` (05.11.2026).

use rust_decimal::Decimal;
use serde::de::{DeserializeOwned, Error as _};
use serde::{Deserialize, Deserializer, Serializer};

/// Decimals the docs allow in a price of an order command.
const COMMAND_DECIMALS: u32 = 8;

/// The number with the trailing zeros past [`COMMAND_DECIMALS`] dropped. A number with more
/// significant decimals is left as it is: the broker refuses it, and that is the right answer.
fn within_command_decimals(value: Decimal) -> Decimal {
    if value.scale() <= COMMAND_DECIMALS {
        return value;
    }
    let short = value.normalize();
    if short.scale() > COMMAND_DECIMALS {
        value
    } else {
        short
    }
}

/// `serialize_with` for an `Option<Decimal>` price of an order command, see the module docs.
pub mod command_number {
    use super::*;

    /// Writes the number as a JSON number, `None` as null.
    pub fn serialize<S: Serializer>(
        value: &Option<Decimal>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        rust_decimal::serde::arbitrary_precision_option::serialize(
            &value.map(within_command_decimals),
            serializer,
        )
    }
}

/// `deserialize_with` for a list that may come as one object: one object is a list of one, null is
/// an empty list.
///
/// Goes through [`serde_json::Value`], not `#[serde(untagged)]`: untagged buffers the input and
/// loses the exact numbers of `arbitrary_precision`.
pub fn one_or_many<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: DeserializeOwned,
{
    match serde_json::Value::deserialize(deserializer)? {
        serde_json::Value::Null => Ok(Vec::new()),
        serde_json::Value::Array(items) => items
            .into_iter()
            .map(|item| serde_json::from_value(item).map_err(D::Error::custom))
            .collect(),
        one => Ok(vec![serde_json::from_value(one).map_err(D::Error::custom)?]),
    }
}

/// `deserialize_with` for an `Option<i32>` code that may come as a JSON integer or as its text.
pub fn int_or_text<'de, D>(deserializer: D) -> Result<Option<i32>, D::Error>
where
    D: Deserializer<'de>,
{
    match serde_json::Value::deserialize(deserializer)? {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::String(text) => text
            .trim()
            .parse()
            .map(Some)
            .map_err(|_| D::Error::custom(format!("not an integer code: {text:?}"))),
        other => serde_json::from_value(other)
            .map(Some)
            .map_err(D::Error::custom),
    }
}
