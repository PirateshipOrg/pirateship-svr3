Rust implementation of the Password Protected Secret Sharing Scheme from Jarecki et al 2016.
This mirrors the C++ implmentation in https://github.com/signalapp/SecureValueRecovery2



# Server init

Each server wakes up and generates (token_sk, token_pk) RSA keys.
Then they store token_pk in consensus kv store, and wait till it gets audited.


# Setting a new key

Client requests leader to reset count to 0.

Client generates (t, n) shares.


Client generates masks for all n shares using OPRF.
This involves sending a request with the password to each server to reset that client's OPRF keys.


Client creates n masked shares.
Client then stores these shares in each server.

Client stores a commitment to the secret key locally.


# Querying a new key

Client queries leader for an allowed attempt.

Leader checks if usage count exceeds max count or not, and creates a token = "ALLOWED" | "DENIED" accordingly, and signs it with its own private key.
Leader samples (t + 1) nodes out of n nodes, creates (t + 1) encrypted tokens using the token_pks of these (t + 1) nodes, and sends these encrypted tokens back to the client.


Client retrieves masked secret shares from these (t + 1) servers.
It then attempts to retreive the masks using the tokens.
If the tokens decrypt to say "ALLOWED", the servers return the masks.

Client then recreates the secret key using the shares,
and checks if the commitment to the secret key matches the saved commitment.

# Querying for counter token

When the leader commits a counter fetch_add request,
the kvs backing the consensus layer checks if there are already MAX_GUESSES - 1 requests for that users that are committed but not audited.
If so, the kvs doesn't send its reply until this request is audited.
This makes sure that, even on equivocation, the maximum times a pin is guessed never goes above the threshold.


Note: We are not doing user auth for now.

Total shared state using consensus: 1 counter per user, 1 public key per server.

