pub type Digest = [u8; 64];

pub struct Keypair {
    pub pubkey: Pubkey,
    pub secret: Secretkey,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Pubkey {
    pub key: [u8; 32],
}

pub struct Secretkey {
    pub key: [u8; 64],
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Signature {
    pub signer: Pubkey,
    pub sig: [u8; 64],
}

impl Keypair {
    pub fn sign(&self, digest: &Digest) -> Signature {
        // Placeholder for now, to be replaced with real signing
        let mut sig = [0u8; 64];
        for i in 0..64 {
            sig[i] = digest[i] ^ self.pubkey.key[i % 32];
        }
        Signature {
            signer: self.pubkey.clone(),
            sig,
        }
    }
}

impl AsRef<[u8]> for Pubkey {
    fn as_ref(&self) -> &[u8] {
        &self.key
    }
}

impl Signature {
    pub fn verify(&self, digest: &Digest) -> bool {
        // Placeholder for now, to be replaced with real verification
        let mut computed_digest = [0u8; 64];
        for i in 0..64 {
            computed_digest[i] = self.sig[i] ^ self.signer.key[i % 32];
        }
        computed_digest == *digest
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sign_and_verify() {
        let pubkey = Pubkey { key: [1u8; 32] };
        let secret = Secretkey { key: [2u8; 64] };
        let keypair = Keypair { pubkey, secret };

        let digest: Digest = [3u8; 64];
        let signature = keypair.sign(&digest);

        assert!(signature.verify(&digest));
    }
}
