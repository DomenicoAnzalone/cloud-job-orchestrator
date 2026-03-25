import jwt
import requests
from functools import lru_cache

TENANT_ID = "1ff1ab6f-5116-43af-a48b-d8da1301df40"
AUDIENCE = "api://52836a8b-6649-49ac-acbe-53caeccd542f"

JWKS_URL = f"https://login.microsoftonline.com/{TENANT_ID}/discovery/v2.0/keys"


@lru_cache()
def get_jwks():
    response = requests.get(JWKS_URL, timeout=10)
    response.raise_for_status()
    return response.json()


def get_public_key(token):
    unverified_header = jwt.get_unverified_header(token)
    kid = unverified_header["kid"]

    jwks = get_jwks()

    for key in jwks["keys"]:
        if key["kid"] == kid:
            return jwt.algorithms.RSAAlgorithm.from_jwk(key)

    raise ValueError("Public key not found")


def validate_token(token):
    public_key = get_public_key(token)

    # decode SENZA issuer
    decoded = jwt.decode(
        token,
        public_key,
        algorithms=["RS256"],
        audience=AUDIENCE,
        options={"verify_iss": False}
    )

    iss = decoded.get("iss")

    VALID_ISSUERS = [
        f"https://login.microsoftonline.com/{TENANT_ID}/v2.0",
        f"https://login.microsoftonline.com/{TENANT_ID}/",
        f"https://sts.windows.net/{TENANT_ID}/",
    ]

    if iss not in VALID_ISSUERS:
        raise ValueError(f"Invalid issuer: {iss}")

    return decoded


def get_user_from_request(req):
    auth_header = req.headers.get("Authorization", "")

    if not auth_header.startswith("Bearer "):
        raise ValueError("Missing or invalid Authorization header")

    token = auth_header.split(" ", 1)[1]

    decoded = validate_token(token)

    user_id = decoded.get("oid")
    tenant_id = decoded.get("tid")
    if not user_id or not tenant_id:
        raise ValueError("Token missing required claims (oid/tid)")

    return {
        "userId": user_id,
        "tenantId": tenant_id,
    }
