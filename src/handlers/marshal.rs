use serde::{Deserialize, Serialize};
use voprf::{BlindedElement, Ristretto255};

use crate::state::ClientId;

#[derive(Serialize, Deserialize)]
pub struct ClientRequest {
    pub client_id: ClientId,

    /// Use base64 encoding for the blinded element.
    #[serde(with = "base64_blinded_element")]
    pub blinded_element: BlindedElement<Ristretto255>,
}


/// blinded_element base64 codec (makes json readable).
/// Derived from: https://users.rust-lang.org/t/serialize-a-vec-u8-to-json-as-base64/57781
mod base64_blinded_element {
    use serde::{Serialize, Deserialize};
    use serde::{Deserializer, Serializer};
    use base64::prelude::*;
    use voprf::{BlindedElement, Ristretto255};

    pub fn serialize<S: Serializer>(v: &BlindedElement<Ristretto255>, s: S) -> Result<S::Ok, S::Error> {
        let ser = v.serialize();
        let base64 = BASE64_STANDARD.encode(ser);
        String::serialize(&base64, s)
    }
    
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<BlindedElement<Ristretto255>, D::Error> {
        let base64 = String::deserialize(d)?;
        let ser = BASE64_STANDARD.decode(base64.as_bytes()).map_err(|e| serde::de::Error::custom(e))?;
        BlindedElement::<Ristretto255>::deserialize(&ser)
            .map_err(|e| serde::de::Error::custom(e))
    }
}
