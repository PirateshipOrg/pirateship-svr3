use serde::{Deserialize, Serialize};
use voprf::{BlindedElement, Ristretto255};
use curve25519_dalek::ristretto::RistrettoPoint;
use voprf::{EvaluationElement, Proof};
use base64::prelude::*;

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


/// Response from the /refresh endpoint.
#[derive(Debug, Clone)]
pub struct RefreshResponse {
    /// The evaluated OPRF result (evaluated element)
    pub evaluate_result: EvaluationElement<Ristretto255>,
    
    /// Proof of correct evaluation
    pub proof: Proof<Ristretto255>,
    
    /// Public key of the OPRF server
    pub public_key: RistrettoPoint,
}

/// Response from the /evaluate endpoint.
#[derive(Debug, Clone)]
pub struct EvaluateResponse {
    /// The evaluated OPRF result (evaluated element)
    pub evaluate_result: EvaluationElement<Ristretto255>,
    
    /// Proof of correct evaluation
    pub proof: Proof<Ristretto255>,
}

// Custom serialization/deserialization for RefreshResponse
// The server sends these as JSON with base64-encoded binary data
impl Serialize for RefreshResponse {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(3))?;
        
        // Serialize evaluate_result as base64
        let eval_ser = self.evaluate_result.serialize();
        let eval_base64 = base64::prelude::BASE64_STANDARD.encode(eval_ser);
        map.serialize_entry("evaluate_result", &eval_base64)?;
        
        // Serialize proof as base64
        let proof_ser = self.proof.serialize();
        let proof_base64 = base64::prelude::BASE64_STANDARD.encode(proof_ser);
        map.serialize_entry("proof", &proof_base64)?;
        
        // Serialize public_key as base64 (RistrettoPoint uses compressed representation)
        let pk_bytes = self.public_key.compress().to_bytes();
        let pk_base64 = base64::prelude::BASE64_STANDARD.encode(pk_bytes);
        map.serialize_entry("public_key", &pk_base64)?;
        
        map.end()
    }
}

impl<'de> Deserialize<'de> for RefreshResponse {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, MapAccess, Visitor};
        use std::fmt;

        struct RefreshResponseVisitor;

        impl<'de> Visitor<'de> for RefreshResponseVisitor {
            type Value = RefreshResponse;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a RefreshResponse object")
            }

            fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut evaluate_result = None;
                let mut proof = None;
                let mut public_key = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "evaluate_result" => {
                            if evaluate_result.is_some() {
                                return Err(de::Error::duplicate_field("evaluate_result"));
                            }
                            let base64_str: String = map.next_value()?;
                            let bytes = base64::prelude::BASE64_STANDARD
                                .decode(base64_str.as_bytes())
                                .map_err(de::Error::custom)?;
                            let eval_elem = EvaluationElement::<Ristretto255>::deserialize(&bytes)
                                .map_err(de::Error::custom)?;
                            evaluate_result = Some(eval_elem);
                        }
                        "proof" => {
                            if proof.is_some() {
                                return Err(de::Error::duplicate_field("proof"));
                            }
                            let base64_str: String = map.next_value()?;
                            let bytes = base64::prelude::BASE64_STANDARD
                                .decode(base64_str.as_bytes())
                                .map_err(de::Error::custom)?;
                            let proof_val = Proof::<Ristretto255>::deserialize(&bytes)
                                .map_err(de::Error::custom)?;
                            proof = Some(proof_val);
                        }
                        "public_key" => {
                            if public_key.is_some() {
                                return Err(de::Error::duplicate_field("public_key"));
                            }
                            let base64_str: String = map.next_value()?;
                            let bytes = base64::prelude::BASE64_STANDARD
                                .decode(base64_str.as_bytes())
                                .map_err(de::Error::custom)?;
                            if bytes.len() != 32 {
                                return Err(de::Error::custom("Invalid RistrettoPoint length"));
                            }
                            let mut pk_bytes = [0u8; 32];
                            pk_bytes.copy_from_slice(&bytes);
                            let compressed = curve25519_dalek::ristretto::CompressedRistretto(pk_bytes);
                            let pk = compressed.decompress()
                                .ok_or_else(|| de::Error::custom("Invalid RistrettoPoint"))?;
                            public_key = Some(pk);
                        }
                        _ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }

                let evaluate_result = evaluate_result.ok_or_else(|| de::Error::missing_field("evaluate_result"))?;
                let proof = proof.ok_or_else(|| de::Error::missing_field("proof"))?;
                let public_key = public_key.ok_or_else(|| de::Error::missing_field("public_key"))?;

                Ok(RefreshResponse {
                    evaluate_result,
                    proof,
                    public_key,
                })
            }
        }

        deserializer.deserialize_map(RefreshResponseVisitor)
    }
}

// Custom serialization/deserialization for EvaluateResponse
impl Serialize for EvaluateResponse {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(2))?;
        
        // Serialize evaluate_result as base64
        let eval_ser = self.evaluate_result.serialize();
        let eval_base64 = base64::prelude::BASE64_STANDARD.encode(eval_ser);
        map.serialize_entry("evaluate_result", &eval_base64)?;
        
        // Serialize proof as base64
        let proof_ser = self.proof.serialize();
        let proof_base64 = base64::prelude::BASE64_STANDARD.encode(proof_ser);
        map.serialize_entry("proof", &proof_base64)?;
        
        map.end()
    }
}

impl<'de> Deserialize<'de> for EvaluateResponse {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, MapAccess, Visitor};
        use std::fmt;

        struct EvaluateResponseVisitor;

        impl<'de> Visitor<'de> for EvaluateResponseVisitor {
            type Value = EvaluateResponse;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("an EvaluateResponse object")
            }

            fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut evaluate_result = None;
                let mut proof = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "evaluate_result" => {
                            if evaluate_result.is_some() {
                                return Err(de::Error::duplicate_field("evaluate_result"));
                            }
                            let base64_str: String = map.next_value()?;
                            let bytes = base64::prelude::BASE64_STANDARD
                                .decode(base64_str.as_bytes())
                                .map_err(de::Error::custom)?;
                            let eval_elem = EvaluationElement::<Ristretto255>::deserialize(&bytes)
                                .map_err(de::Error::custom)?;
                            evaluate_result = Some(eval_elem);
                        }
                        "proof" => {
                            if proof.is_some() {
                                return Err(de::Error::duplicate_field("proof"));
                            }
                            let base64_str: String = map.next_value()?;
                            let bytes = base64::prelude::BASE64_STANDARD
                                .decode(base64_str.as_bytes())
                                .map_err(de::Error::custom)?;
                            let proof_val = Proof::<Ristretto255>::deserialize(&bytes)
                                .map_err(de::Error::custom)?;
                            proof = Some(proof_val);
                        }
                        _ => {
                            let _ = map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }

                let evaluate_result = evaluate_result.ok_or_else(|| de::Error::missing_field("evaluate_result"))?;
                let proof = proof.ok_or_else(|| de::Error::missing_field("proof"))?;

                Ok(EvaluateResponse {
                    evaluate_result,
                    proof,
                })
            }
        }

        deserializer.deserialize_map(EvaluateResponseVisitor)
    }
}
