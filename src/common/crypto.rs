use ed25519_dalek::{ed25519::{self, signature::Signer}, SigningKey, VerifyingKey};
use rand::rngs::OsRng;

pub type Digest = [u8; 64];

pub struct Keypair {
    pubkey: Pubkey,
    dalek_signer: SigningKey,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Pubkey {
    pub key: [u8; 32],
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Signature {
    pub signer: Pubkey,
    pub sig: [u8; 64],
}

impl Keypair {
    pub fn new_pair() -> Self {
        let mut csprng = OsRng;
        let dalek_signer = SigningKey::generate(&mut csprng);
        let pubkey = Pubkey::from(&dalek_signer.verifying_key().to_bytes());

        Keypair {
            pubkey,
            dalek_signer,
        }
    }

    pub fn sign(&self, digest: &Digest) -> Signature {
        let sig = self
            .dalek_signer
            .sign(digest)
            .to_bytes();

        Signature {
            signer: self.pubkey.clone(),
            sig,
        }
    }

    pub fn pubkey(&self) -> Pubkey {
        self.pubkey.clone()
    }
}

impl AsRef<[u8]> for Pubkey {
    fn as_ref(&self) -> &[u8] {
        &self.key
    }
}

impl From<&[u8; 32]> for Pubkey {
    fn from(bytes: &[u8; 32]) -> Self {
        let mut key = [0u8; 32];
        key.copy_from_slice(&bytes[..32]);
        Pubkey { key }
    }
}

impl Signature {
    pub fn verify(&self, digest: &Digest) -> bool {
        let dalek_sig = ed25519::Signature::from_bytes(&self.sig);
        let dalek_pubkey = VerifyingKey::from_bytes(&self.signer.key).unwrap();
        dalek_pubkey
            .verify_strict(digest, &dalek_sig)
            .is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sign_and_verify() {
        let keypair = Keypair::new_pair();
        let digest: Digest = [4; 64];
        let signature = keypair.sign(&digest);

        assert_eq!(signature.signer, keypair.pubkey);
        assert!(signature.verify(&digest));
    }
}
