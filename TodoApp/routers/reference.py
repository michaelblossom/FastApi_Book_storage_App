i have followed all the steps , suggestion and code structure you provided,

i want to paste all the files here again for final confirmation

the code below is from paystack_service.py file

"""Service abstraction for interacting with the Paystack API."""





from __future__ import annotations

import httpx

from typing import Any

from uuid import UUID

from dataclasses import dataclass

from src.core.logger import get_logger

from src.core.settings import Settings





# Define or import PaystackServiceError if not already present

class PaystackServiceError(Exception):

    """Base exception for Paystack service errors."""

    pass



# Define or import PaystackInitializationError if not already present

class PaystackInitializationError(PaystackServiceError):

    pass



class PaystackService:

    async def initialize_subscription(

        self,

        *,

        email: str,

        plan_code: str,

        amount: float | int,  # Explicitly passed major unit (e.g., 10000.00 NGN)

        reference: str,

        metadata: dict[str, Any],

        client: httpx.AsyncClient | None = None,

    ) -> 'PaystackSetupIntent':

        """Initializes a transaction tied to a specific recurring plan with explicit amount."""

        amount_kobo = int(round(float(amount) * 100))

        payload = {

            "email": email,

            "plan": plan_code,

            "amount": amount_kobo,

            "reference": reference,

            "metadata": metadata,

        }

        headers = self._build_headers()

        logger = get_logger()

        logger.info(

            "paystack_initialize_subscription_request",

            extra={

                "email": email,

                "plan_code": plan_code,

                "amount_kobo": amount_kobo,

                "reference": reference,

            },

        )

        async def _execute(client_obj: httpx.AsyncClient) -> 'PaystackSetupIntent':

            response = await client_obj.post(

                f"{self._BASE_URL}/transaction/initialize",

                json=payload,

                headers=headers,

            )

            if response.status_code >= 400:

                message = self._extract_error_message(response)

                logger.warning(

                    "paystack_initialize_subscription_http_error",

                    extra={

                        "email": email,

                        "status_code": response.status_code,

                        "error_message": message,

                    },

                )

                raise PaystackInitializationError(message)

            data = response.json()

            if not data.get("status"):

                raise PaystackInitializationError(

                    data.get("message") or "Paystack subscription initialization failed"

                )

            initialisation = data.get("data") or {}

            authorization_url = initialisation.get("authorization_url")

            access_code = initialisation.get("access_code")

            reference_value = initialisation.get("reference")

            if not authorization_url or not access_code or not reference_value:

                raise PaystackInitializationError(

                    "Paystack subscription initialization response missing required fields"

                )

            return PaystackSetupIntent(

                authorization_url=authorization_url,

                access_code=access_code,

                reference=reference_value,

            )

        if client is not None:

            return await _execute(client)

        async with httpx.AsyncClient(timeout=20.0) as async_client:

            return await _execute(async_client)

class PaystackVerificationError(Exception):

    """Raised when a transaction verification fails."""



@dataclass(frozen=True)

class PaystackSetupIntent:

    """Represents the response payload required to kick off Paystack checkout."""

    authorization_url: str

    access_code: str

    reference: str



@dataclass(frozen=True)

class PaystackAuthorization:

    """Tokenised card details returned upon successful verification."""

    authorization_code: str

    customer_code: str

    last4: str | None

    card_type: str | None

    bank: str | None

    country_code: str | None

    exp_month: int | None

    exp_year: int | None

    bin: str | None



@dataclass(frozen=True)

class PaystackVerification:

    """Structured result returned from a Paystack verification call."""

    reference: str

    status: str

    amount: int

    currency: str

    authorization: PaystackAuthorization



@dataclass(frozen=True)

class PaystackBank:

    """Represents basic information about a settlement bank supported by Paystack."""

    name: str

    code: str

    longcode: str | None

    type: str | None



@dataclass(frozen=True)

class PaystackResolvedAccount:

    """Result returned when validating a bank account with Paystack."""

    account_name: str

    account_number: str

    bank_code: str



@dataclass(frozen=True)

class PaystackSubAccount:

    """Paystack subaccount metadata used for settlement splits."""

    subaccount_code: str

    business_name: str

    settlement_bank: str

    account_number: str

    account_name: str | None

    percentage_charge: float

    currency: str





class PaystackService:

    """Typed, logged wrapper around Paystack's REST API."""



    _BASE_URL = "https://api.paystack.co"



    def __init__(self, settings: Settings) -> None:

        if not settings.PAYSTACK_TEST_SECRET_KEY:

            raise ValueError("Paystack secret key is not configured")



        self._settings = settings

        self._secret_key = settings.PAYSTACK_TEST_SECRET_KEY

        self._callback_url = settings.PAYSTACK_CALLBACK_URL



    async def initialize_payment_method(

        self,

        *,

        tenant_id: UUID,

        email: str,

        currency: str,

        amount_kobo: int,

        metadata: dict[str, Any] | None = None,

        reference: str | None = None,

        callback_url: str | None = None,

        channels: list[str] | None = None,

        client: httpx.AsyncClient | None = None,

    ) -> PaystackSetupIntent:

        """Create a Paystack transaction initialisation for card tokenisation."""



        callback_value = callback_url or self._callback_url



        payload: dict[str, Any] = {

            "email": email,

            "amount": amount_kobo,

            "currency": currency,

            "reference": reference or self._generate_reference(tenant_id),

            "metadata": self._normalise_metadata(metadata, tenant_id),

        }



        if channels:

            payload["channels"] = channels



        if callback_value:

            payload["callback_url"] = str(callback_value)



        headers = self._build_headers()



        logger.info(

            "paystack_initialize_request",

            extra={

                "tenant_id": str(tenant_id),

                "currency": currency,

                "callback_url": payload["callback_url"],

            },

        )



        async def _execute(client_obj: httpx.AsyncClient) -> PaystackSetupIntent:

            response = await client_obj.post(

                f"{self._BASE_URL}/transaction/initialize",

                json=payload,

                headers=headers,

            )



            if response.status_code >= 400:

                message = self._extract_error_message(response)

                logger.warning(

                    "paystack_initialize_http_error",

                    extra={

                        "tenant_id": str(tenant_id),

                        "status_code": response.status_code,

                        "error_message": message,

                    },

                )

                raise PaystackInitializationError(message)



            data = response.json()

            if not data.get("status"):

                raise PaystackInitializationError(

                    data.get("message") or "Paystack initialization failed"

                )



            initialisation = data.get("data") or {}

            authorization_url = initialisation.get("authorization_url")

            access_code = initialisation.get("access_code")

            reference_value = initialisation.get("reference")



            if not authorization_url or not access_code or not reference_value:

                raise PaystackInitializationError(

                    "Paystack initialization response missing required fields"

                )



            logger.info(

                "paystack_initialize_success",

                extra={

                    "tenant_id": str(tenant_id),

                    "reference": reference_value,

                },

            )



            return PaystackSetupIntent(

                authorization_url=authorization_url,

                access_code=access_code,

                reference=reference_value,

            )



        if client is not None:

            return await _execute(client)



        async with httpx.AsyncClient(timeout=20.0) as async_client:

            return await _execute(async_client)



    async def verify_transaction(

        self,

        *,

        reference: str,

        client: httpx.AsyncClient | None = None,

    ) -> PaystackVerification:

        """Verify a Paystack transaction and extract tokenised card details."""



        headers = self._build_headers()

        logger.info(

            "paystack_verify_request",

            extra={"reference": reference},

        )



        async def _execute(client_obj: httpx.AsyncClient) -> PaystackVerification:

            response = await client_obj.get(

                f"{self._BASE_URL}/transaction/verify/{reference}",

                headers=headers,

            )

            if response.status_code >= 400:

                message = self._extract_error_message(response)

                logger.warning(

                    "paystack_verify_http_error",

                    extra={

                        "reference": reference,

                        "status_code": response.status_code,

                        "error_message": message,

                    },

                )

                raise PaystackVerificationError(message)

            body = response.json()



            if not body.get("status"):

                raise PaystackVerificationError(

                    body.get("message") or "Paystack verification failed"

                )



            data = body.get("data") or {}

            if data.get("status") != "success":

                raise PaystackVerificationError(

                    f"Transaction not successful: {data.get('status')}"

                )



            authorization_payload = data.get("authorization") or {}

            customer_payload = data.get("customer") or {}



            authorization_code = authorization_payload.get("authorization_code")

            customer_code = customer_payload.get("customer_code")



            if not authorization_code or not customer_code:

                raise PaystackVerificationError(

                    "Verification response missing authorization details"

                )



            authorization = PaystackAuthorization(

                authorization_code=authorization_code,

                customer_code=customer_code,

                last4=authorization_payload.get("last4"),

                card_type=authorization_payload.get("card_type")

                or authorization_payload.get("brand"),

                bank=authorization_payload.get("bank"),

                country_code=authorization_payload.get("country_code"),

                exp_month=self._safe_int(authorization_payload.get("exp_month")),

                exp_year=self._safe_int(authorization_payload.get("exp_year")),

                bin=authorization_payload.get("bin"),

            )



            verification = PaystackVerification(

                reference=data.get("reference", reference),

                status=data.get("status", "unknown"),

                amount=int(data.get("amount", 0)),

                currency=data.get("currency", "NGN"),

                authorization=authorization,

            )



            logger.info(

                "paystack_verify_success",

                extra={

                    "reference": verification.reference,

                    "authorization_code": authorization.authorization_code,

                },

            )



            return verification



        if client is not None:

            return await _execute(client)



        async with httpx.AsyncClient(timeout=20.0) as async_client:

            return await _execute(async_client)



    async def list_banks(

        self,

        *,

        currency: str,

        country: str = "nigeria",

        client: httpx.AsyncClient | None = None,

    ) -> list[PaystackBank]:

        """Retrieve settlement banks supported by Paystack for a currency."""



        headers = self._build_headers()

        params = {"currency": currency.upper()}

        if country:

            params["country"] = country



        logger.info(

            "paystack_list_banks_request",

            extra={"currency": currency.upper(), "country": country},

        )



        async def _execute(client_obj: httpx.AsyncClient) -> list[PaystackBank]:

            response = await client_obj.get(

                f"{self._BASE_URL}/bank", params=params, headers=headers

            )

            if response.status_code >= 400:

                message = self._extract_error_message(response)

                logger.warning(

                    "paystack_list_banks_http_error",

                    extra={

                        "currency": currency.upper(),

                        "status_code": response.status_code,

                        "error_message": message,

                    },

                )

                raise PaystackServiceError(message)



            payload = response.json()

            if not payload.get("status"):

                raise PaystackServiceError(

                    payload.get("message") or "Unable to fetch banks from Paystack"

                )



            banks: list[PaystackBank] = []

            for item in payload.get("data", []):

                banks.append(

                    PaystackBank(

                        name=item.get("name", ""),

                        code=item.get("code", ""),

                        longcode=item.get("longcode"),

                        type=item.get("type"),

                    )

                )



            logger.info(

                "paystack_list_banks_success",

                extra={

                    "currency": currency.upper(),

                    "bank_count": len(banks),

                },

            )



            return banks



        if client is not None:

            return await _execute(client)



        async with httpx.AsyncClient(timeout=20.0) as async_client:

            return await _execute(async_client)



    async def resolve_account(

        self,

        *,

        account_number: str,

        bank_code: str,

        client: httpx.AsyncClient | None = None,

    ) -> PaystackResolvedAccount:

        """Resolve a bank account number to confirm the registered account name."""



        headers = self._build_headers()

        params = {"account_number": account_number, "bank_code": bank_code}

        logger.info(

            "paystack_resolve_account_request",

            extra={"bank_code": bank_code},

        )



        async def _execute(client_obj: httpx.AsyncClient) -> PaystackResolvedAccount:

            response = await client_obj.get(

                f"{self._BASE_URL}/bank/resolve", params=params, headers=headers

            )

            if response.status_code >= 400:

                message = self._extract_error_message(response)

                logger.warning(

                    "paystack_resolve_account_http_error",

                    extra={

                        "bank_code": bank_code,

                        "status_code": response.status_code,

                        "error_message": message,

                    },

                )

                raise PaystackServiceError(message)



            payload = response.json()

            if not payload.get("status"):

                raise PaystackServiceError(

                    payload.get("message") or "Unable to resolve bank account"

                )



            data = payload.get("data") or {}

            account_name = data.get("account_name")

            resolved_account_number = data.get("account_number") or account_number



            if not account_name:

                raise PaystackServiceError(

                    "Paystack did not return an account name during resolution"

                )



            logger.info(

                "paystack_resolve_account_success",

                extra={

                    "bank_code": bank_code,

                },

            )



            return PaystackResolvedAccount(

                account_name=account_name,

                account_number=resolved_account_number,

                bank_code=bank_code,

            )



        if client is not None:

            return await _execute(client)



        async with httpx.AsyncClient(timeout=20.0) as async_client:

            return await _execute(async_client)



    async def create_subaccount(

        self,

        *,

        business_name: str,

        bank_code: str,

        account_number: str,

        percentage_charge: float,

        currency: str,

        account_name: str | None = None,

        email: str | None = None,

        settlement_schedule: str = "AUTO",

        metadata: dict[str, Any] | None = None,

        client: httpx.AsyncClient | None = None,

    ) -> PaystackSubAccount:

        """Create a Paystack subaccount used for splitting settlements."""



        headers = self._build_headers()

        payload: dict[str, Any] = {

            "business_name": business_name,

            "settlement_bank": bank_code,

            "account_number": account_number,

            "percentage_charge": percentage_charge,

            "currency": currency.upper(),

            "settlement_schedule": settlement_schedule,

        }

        if account_name:

            payload["account_name"] = account_name

        if email:

            payload["primary_contact_email"] = email

        if metadata:

            payload["metadata"] = metadata



        logger.info(

            "paystack_create_subaccount_request",

            extra={"business_name": business_name, "bank_code": bank_code},

        )



        async def _execute(client_obj: httpx.AsyncClient) -> PaystackSubAccount:

            response = await client_obj.post(

                f"{self._BASE_URL}/subaccount", json=payload, headers=headers

            )

            if response.status_code >= 400:

                message = self._extract_error_message(response)

                logger.warning(

                    "paystack_create_subaccount_http_error",

                    extra={

                        "business_name": business_name,

                        "status_code": response.status_code,

                        "error_message": message,

                    },

                )

                raise PaystackServiceError(message)



            body = response.json()

            if not body.get("status"):

                raise PaystackServiceError(

                    body.get("message") or "Paystack subaccount creation failed"

                )



            data = body.get("data") or {}

            subaccount_code = data.get("subaccount_code")

            settlement_bank = data.get("settlement_bank") or bank_code

            returned_account_number = data.get("account_number") or account_number

            returned_account_name = data.get("account_name") or account_name

            percentage = float(data.get("percentage_charge", percentage_charge))

            currency_code = data.get("currency", currency.upper())



            if not subaccount_code:

                raise PaystackServiceError(

                    "Paystack subaccount response missing subaccount_code"

                )



            logger.info(

                "paystack_create_subaccount_success",

                extra={

                    "business_name": business_name,

                    "subaccount_code": subaccount_code,

                },

            )



            return PaystackSubAccount(

                subaccount_code=subaccount_code,

                business_name=data.get("business_name") or business_name,

                settlement_bank=settlement_bank,

                account_number=returned_account_number,

                account_name=returned_account_name,

                percentage_charge=percentage,

                currency=currency_code,

            )



        if client is not None:

            return await _execute(client)



        async with httpx.AsyncClient(timeout=20.0) as async_client:

            return await _execute(async_client)



    async def initialize_subscription(

        self,

        email: str,

        plan_code: str,  # The Paystack Plan ID (PLN_...)

        reference: str,

        metadata: dict,

    ) -> PaystackSetupIntent:

        """Initializes a transaction tied to a specific recurring plan."""



        # Extract amount from metadata or add as argument if needed

        amount = metadata.get("price")

        if amount is not None:

            try:

                # Convert to kobo if NGN

                amount_kobo = int(float(amount) * 100)

            except Exception:

                amount_kobo = amount

        else:

            amount_kobo = None



        payload = {

            "email": email,

            "plan": plan_code,

            "reference": reference,

            "metadata": metadata,

        }

        if amount_kobo is not None:

            payload["amount"] = amount_kobo

        headers = self._build_headers()



        logger.info(

            "paystack_initialize_subscription_request",

            extra={

                "email": email,

                "plan_code": plan_code,

                "reference": reference,

            },

        )



        async def _execute(client_obj: httpx.AsyncClient) -> PaystackSetupIntent:

            response = await client_obj.post(

                f"{self._BASE_URL}/transaction/initialize",

                json=payload,

                headers=headers,

            )



            if response.status_code >= 400:

                message = self._extract_error_message(response)

                logger.warning(

                    "paystack_initialize_subscription_http_error",

                    extra={

                        "email": email,

                        "status_code": response.status_code,

                        "error_message": message,

                    },

                )

                raise PaystackInitializationError(message)



            data = response.json()

            if not data.get("status"):

                raise PaystackInitializationError(

                    data.get("message")

                    or "Paystack subscription initialization failed"

                )



            initialisation = data.get("data") or {}

            authorization_url = initialisation.get("authorization_url")

            access_code = initialisation.get("access_code")

            reference_value = initialisation.get("reference")



            if not authorization_url or not access_code or not reference_value:

                raise PaystackInitializationError(

                    "Paystack subscription initialization response missing required fields"

                )



            logger.info(

                "paystack_initialize_subscription_success",

                extra={

                    "email": email,

                    "reference": reference_value,

                },

            )



            return PaystackSetupIntent(

                authorization_url=authorization_url,

                access_code=access_code,

                reference=reference_value,

            )



        client = None

        if client is not None:

            return await _execute(client)



        async with httpx.AsyncClient(timeout=20.0) as async_client:

            return await _execute(async_client)



    async def update_subscription(self, subscription_code: str, plan_code: str) -> dict[str, any]:

        """

        Updates an existing subscription to a new plan.

        Reference: https://paystack.com/docs/api/subscription/#update

        """

        headers = self._build_headers()

        payload = {"code": subscription_code, "plan": plan_code}



        async with httpx.AsyncClient(timeout=20.0) as client:

            response = await client.post(

                f"{self._BASE_URL}/subscription/update",

                json=payload,

                headers=headers

            )

            if response.status_code >= 400:

                message = self._extract_error_message(response)

                logger.error(f"Paystack subscription update failed: {message}")

                raise PaystackServiceError(f"Upgrade failed: {message}")

            return response.json()



    async def cancel_subscription(self, subscription_code: str, email_token: str):

        """Disables a subscription on Paystack."""

        headers = self._build_headers()

        payload = {"code": subscription_code, "token": email_token}



        async with httpx.AsyncClient(timeout=20.0) as client:

            response = await client.post(

                f"{self._BASE_URL}/subscription/disable", json=payload, headers=headers

            )

            if response.status_code >= 400:

                raise PaystackServiceError(f"Cancellation failed: {response.text}")

            return response.json()



    def _build_headers(self) -> dict[str, str]:

        return {

            "Authorization": f"Bearer {self._secret_key}",

            "Content-Type": "application/json",

        }



    @staticmethod

    def _generate_reference(tenant_id: UUID) -> str:

        return f"{tenant_id.hex}-{uuid4().hex}"



    @staticmethod

    def _safe_int(value: Any) -> int | None:

        try:

            if value is None or value == "":

                return None

            return int(value)

        except (TypeError, ValueError):  # pragma: no cover - defensive guard

            return None



    @staticmethod

    def _extract_error_message(response: httpx.Response) -> str:

        try:

            payload = response.json()

        except ValueError:  # pragma: no cover - defensive guard

            return (

                response.text

                or f"Paystack request failed with status {response.status_code}"

            )



        if isinstance(payload, dict):

            message = payload.get("message")

            errors = payload.get("errors")

            if isinstance(errors, list) and errors:

                detailed = ", ".join(str(item) for item in errors)

                message = f"{message} ({detailed})" if message else detailed

            elif isinstance(errors, dict) and errors:

                detailed = ", ".join(f"{key}: {value}" for key, value in errors.items())

                message = f"{message} ({detailed})" if message else detailed

            if message:

                return str(message)

        return f"Paystack request failed with status {response.status_code}"



    @staticmethod

    def _normalise_metadata(

        metadata: dict[str, Any] | None, tenant_id: UUID

    ) -> dict[str, Any]:

        base = metadata or {"tenant_id": str(tenant_id)}

        normalised: dict[str, Any] = {}

        for key, value in base.items():

            if isinstance(value, (str, int, float, bool)) or value is None:

                normalised[key] = value

            else:

                normalised[key] = str(value)

        return normalised

the code below is from onboarding.py file under model folder

"""ORM models supporting merchant onboarding progress."""



from __future__ import annotations



from datetime import datetime



from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, UUID

from sqlalchemy.orm import Mapped, mapped_column

from sqlalchemy.sql import func



from src.models.base import Base



DEFAULT_PROFILE_DOCUMENT_TYPE = "UNSPECIFIED"





class MerchantProfile(Base):

    """Stores profile details captured during onboarding."""



    __tablename__ = "merchant_profiles"



    tenant_id: Mapped[UUID] = mapped_column(

        UUID(as_uuid=True),

        ForeignKey("merchants.id", ondelete="CASCADE"),

        primary_key=True,

    )

    business_name: Mapped[str] = mapped_column(String(255), nullable=False)

    business_type: Mapped[str] = mapped_column(String(128), nullable=False)

    business_registration_number: Mapped[str | None] = mapped_column(

        String(128), nullable=True

    )

    phone_number: Mapped[str] = mapped_column(String(32), nullable=False)

    email: Mapped[str] = mapped_column(String(255), nullable=False)

    business_address: Mapped[str] = mapped_column(String(500), nullable=False)

    document_type: Mapped[str] = mapped_column(

        String(128),

        nullable=False,

        default=DEFAULT_PROFILE_DOCUMENT_TYPE,

        server_default=DEFAULT_PROFILE_DOCUMENT_TYPE,

    )

    document_file: Mapped[str | None] = mapped_column(String(2048), nullable=True)

    live_enabled: Mapped[bool] = mapped_column(

        Boolean, nullable=False, default=False, server_default="false"

    )

    created_at: Mapped[datetime] = mapped_column(

        DateTime(timezone=True), nullable=False, server_default=func.now()

    )

    updated_at: Mapped[datetime] = mapped_column(

        DateTime(timezone=True),

        nullable=False,

        server_default=func.now(),

        onupdate=datetime.utcnow,

    )



    def __repr__(self) -> str:

        return "<MerchantProfile(tenant_id={tenant_id}, business_name={business_name})>".format(

            tenant_id=self.tenant_id,

            business_name=self.business_name,

        )





class MerchantWabaLink(Base):

    """Represents the WhatsApp Business Account linkage state for a tenant."""



    __tablename__ = "merchant_waba_links"



    tenant_id: Mapped[UUID] = mapped_column(

        UUID(as_uuid=True),

        ForeignKey("merchants.id", ondelete="CASCADE"),

        primary_key=True,

    )

    waba_phone: Mapped[str] = mapped_column(String(32), nullable=False)

    is_verified: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    twilio_sid: Mapped[str | None] = mapped_column(String(64), nullable=True)



    def __repr__(self) -> str:

        return "<MerchantWabaLink(tenant_id={tenant_id}, phone={phone}, verified={verified})>".format(

            tenant_id=self.tenant_id,

            phone=self.waba_phone,

            verified=self.is_verified,

        )





class MerchantPlan(Base):

    """Captures the chosen subscription plan for a merchant tenant."""



    __tablename__ = "merchant_plans"



    tenant_id: Mapped[UUID] = mapped_column(

        UUID(as_uuid=True),

        ForeignKey("merchants.id", ondelete="CASCADE"),

        primary_key=True,

    )

    plan_code: Mapped[str] = mapped_column(String(64), nullable=False)



    # Status field to support Webhook updates (ACTIVE, CANCELLED, etc)

    status: Mapped[str] = mapped_column(

        String(32), default="PENDING", nullable=False, server_default="PENDING"

    )



    is_active: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    currency_lock: Mapped[str | None] = mapped_column(String(3), nullable=True)

    billing_interval: Mapped[str | None] = mapped_column(String(16), nullable=True)

    billing_cycle_start: Mapped[datetime | None] = mapped_column(

        DateTime(timezone=True), nullable=True

    )

    quota_messages: Mapped[int | None] = mapped_column(Integer, nullable=True)

    price: Mapped[int | None] = mapped_column(Integer, nullable=True)

    cycle_reset_at: Mapped[datetime | None] = mapped_column(

        DateTime(timezone=True), nullable=True

    )

    overage_count: Mapped[int] = mapped_column(

        Integer, default=0, nullable=False, server_default="0"

    )



    # Paystack specific tracking

    paystack_plan_id: Mapped[str | None] = mapped_column(String(64), nullable=True)

    paystack_subscription_code: Mapped[str | None] = mapped_column(

        String(64), nullable=True

    )



    # Required for programmatic cancellation via Paystack API

    paystack_email_token: Mapped[str | None] = mapped_column(String(128), nullable=True)



    def __repr__(self) -> str:

        return "<MerchantPlan(tenant_id={tenant_id}, plan_code={plan_code}, status={status}, active={active})>".format(

            tenant_id=self.tenant_id,

            plan_code=self.plan_code,

            status=self.status,

            active=self.is_active,

        )





class MerchantPaymentMethod(Base):

    """Records the primary payment method configured during onboarding."""



    __tablename__ = "merchant_payment_methods"



    tenant_id: Mapped[UUID] = mapped_column(

        UUID(as_uuid=True),

        ForeignKey("merchants.id", ondelete="CASCADE"),

        primary_key=True,

    )

    provider: Mapped[str] = mapped_column(String(64), nullable=False)

    authorization_code: Mapped[str | None] = mapped_column(String(128), nullable=True)

    customer_code: Mapped[str | None] = mapped_column(String(128), nullable=True)

    last4: Mapped[str | None] = mapped_column(String(4), nullable=True)

    card_type: Mapped[str | None] = mapped_column(String(32), nullable=True)

    bank: Mapped[str | None] = mapped_column(String(64), nullable=True)

    country_code: Mapped[str | None] = mapped_column(String(3), nullable=True)

    exp_month: Mapped[int | None] = mapped_column(Integer, nullable=True)

    exp_year: Mapped[int | None] = mapped_column(Integer, nullable=True)

    bin: Mapped[str | None] = mapped_column(String(8), nullable=True)

    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    created_at: Mapped[datetime] = mapped_column(

        DateTime(timezone=True),

        nullable=False,

        default=datetime.utcnow,

        server_default=func.now(),

    )

    updated_at: Mapped[datetime] = mapped_column(

        DateTime(timezone=True),

        nullable=False,

        default=datetime.utcnow,

        onupdate=datetime.utcnow,

        server_default=func.now(),

    )



    def __repr__(self) -> str:

        return "<MerchantPaymentMethod(tenant_id={tenant_id}, provider={provider}, active={active})>".format(

            tenant_id=self.tenant_id,

            provider=self.provider,

            active=self.is_active,

        )

the code below is from billing.py file under endpoint folder

from __future__ import annotations



import hashlib

import hmac

import logging

from datetime import datetime, timedelta

from uuid import UUID



from fastapi import (

    APIRouter,

    BackgroundTasks,

    Depends,

    HTTPException,

    Request,

    status,

)

from sqlalchemy import select, update

from sqlalchemy.ext.asyncio import AsyncSession



# Internal Imports

from src.api.dependencies.auth import get_current_session, require_tenant_id



from src.services.plan_service import PlanService

from src.core.database import get_db_session

from src.core.settings import get_settings

from src.models.billing import InvoiceCategory, InvoiceStatus, MerchantInvoice

from src.models.onboarding import MerchantPlan

from src.schemas.billing import (

    BillingActivityItem,

    PlanIntentRequest,

    PlanIntentResponse,

    SubscriptionCheckoutRequest,

    SubscriptionCheckoutResponse,

)

from src.services.audit_log_service import AuditLogService

from src.services.invoice_service import InvoiceService

from src.services.merchant_profile_service import MerchantProfileService

from src.services.paystack_service import PaystackService, PaystackServiceError

from src.services.plan_intent_service import (

    PlanCurrencyMismatchError,

    PlanIntentResult,

    PlanIntentService,

    PlanIntervalUnsupportedError,

    PlanNotFoundError,

    PlanPrerequisiteError,

)

from src.services.session_service import SessionInfo

from src.services.billing_history_service import BillingHistoryService



logger = logging.getLogger(__name__)



router = APIRouter(prefix="/billing", tags=["Billing"])





@router.post("/paystack/webhook")

async def paystack_webhook(

    request: Request,

    background_tasks: BackgroundTasks,

    db: AsyncSession = Depends(get_db_session),

    settings=Depends(get_settings),

):

    """Consolidated, secure, and idempotent Paystack Webhook handler."""

    # 1. READ RAW PAYLOAD ONCE

    raw_payload = await request.body()

    paystack_signature = request.headers.get("x-paystack-signature")



    # 2. SIGNATURE VERIFICATION (Done once at the entry point)

    if not paystack_signature:

        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing signature")



    computed_hmac = hmac.new(

        settings.PAYSTACK_SECRET_KEY.encode("utf-8"), 

        raw_payload, 

        hashlib.sha512

    ).hexdigest()



    if not hmac.compare_digest(computed_hmac, paystack_signature):

        logger.error("webhook_signature_mismatch")

        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid signature")



    # 3. PARSE DATA

    payload = await request.json()

    event = payload.get("event")

    data = payload.get("data", {})



    logger.info(f"processing_paystack_webhook: {event}")



    # -------------------------------------------------------------------------

    # EVENT ROUTING (The "Switch" Case)

    # -------------------------------------------------------------------------



    # A. INITIAL SUBSCRIPTION CREATION

    if event == "subscription.create":

        tenant_id = data.get("metadata", {}).get("tenant_id")

        if tenant_id:

            await db.execute(

                update(MerchantPlan)

                .where(MerchantPlan.tenant_id == UUID(tenant_id))

                .values(

                    paystack_subscription_code=data.get("subscription_code"),

                    paystack_email_token=data.get("email_token"),

                    status="PENDING_ACTIVATION"

                )

            )

            await db.commit()

        return {"status": "subscription_linked"}



    # B. SUCCESSFUL PAYMENTS (Charge or Renewal)

    elif event in ["invoice.payment_succeeded", "charge.success"]:

        reference = data.get("reference")

        # IDEMPOTENCY CHECK: Find the invoice

        stmt = select(MerchantInvoice).where(MerchantInvoice.id == UUID(reference))

        res = await db.execute(stmt)

        invoice = res.scalar_one_or_none()



        # If already PAID or missing, skip processing

        if not invoice or invoice.status == InvoiceStatus.PAID.value:

            return {"status": "skipped", "reason": "already_processed_or_not_found"}



        # SECURITY: Integer amount validation (Kobo)

        paystack_amount_kobo = data.get("amount")

        expected_kobo = int(invoice.amount * 100)

        if paystack_amount_kobo < expected_kobo:

            logger.error(f"payment_amount_mismatch: {reference}")

            return {"status": "error", "message": "insufficient_amount"}



        # ATOMIC UPDATE: Mark invoice PAID and activate Plan

        await InvoiceService.mark_invoice_paid(invoice_id=invoice.id, db=db)



        sub_data = data.get("subscription", {})

        # Synchronize Plan Metadata & Quota

        new_paystack_plan_id = sub_data.get("plan", {}).get("plan_code")

        plan_def = PlanService.get_plan_by_paystack_id(new_paystack_plan_id)

        internal_plan_code = plan_def.plan_code if plan_def else invoice.metadata_json.get("plan_id")

        quota_messages = plan_def.quota_messages if plan_def else 0



        next_payment = sub_data.get("next_payment_date")

        cycle_end = datetime.fromisoformat(next_payment.replace("Z", "+00:00")) if next_payment else (datetime.utcnow() + timedelta(days=30))



        await db.execute(

            update(MerchantPlan)

            .where(MerchantPlan.tenant_id == invoice.tenant_id)

            .values(

                is_active=True,

                status="ACTIVE",

                plan_code=internal_plan_code,

                quota_messages=quota_messages,

                paystack_subscription_code=sub_data.get("subscription_code") or data.get("subscription_code"),

                paystack_email_token=sub_data.get("email_token"),

                cycle_reset_at=cycle_end

            )

        )



        background_tasks.add_task(

            AuditLogService.record_plan_change,

            tenant_id=invoice.tenant_id,

            actor_type="SYSTEM",

            new_plan={"plan_id": internal_plan_code, "status": "ACTIVE", "quota_messages": quota_messages},

            db=db,

        )

        await db.commit()

        return {"status": "success"}



    # C. PAYMENT FAILURE

    elif event == "invoice.payment_failed":

        sub_code = data.get("subscription_code")

        # 1. Fetch the plan record to see how many attempts have already failed

        stmt = select(MerchantPlan).where(MerchantPlan.paystack_subscription_code == sub_code)

        res = await db.execute(stmt)

        plan_record = res.scalar_one_or_none()



        if plan_record:

            # 2. Logic: Should we cut them off now or give them a grace period?

            # For this implementation, we set is_active=False to prevent message overages

            await db.execute(

                update(MerchantPlan)

                .where(MerchantPlan.tenant_id == plan_record.tenant_id)

                .values(

                    status="PAST_DUE",

                    is_active=False, # Cutting off access until they update payment

                )

            )



            # 3. SIDE EFFECT: Audit Log for Customer Support visibility

            background_tasks.add_task(

                AuditLogService.record_plan_change,

                tenant_id=plan_record.tenant_id,

                actor_type="SYSTEM",

                actor_id=None,

                previous_plan={"status": plan_record.status},

                new_plan={"status": "PAST_DUE", "reason": "payment_failed"},

                db=db,

            )



            await db.commit()

            logger.warning(f"access_restricted_payment_failed: {sub_code}")

            return {"status": "marked_past_due"}



    # D. SOFT CANCELLATION

    elif event == "subscription.disable":

        sub_code = data.get("subscription_code")

        await db.execute(

            update(MerchantPlan)

            .where(MerchantPlan.paystack_subscription_code == sub_code)

            .values(status="CANCELLING") # is_active stays True until cleanup script runs

        )

        await db.commit()

        return {"status": "soft_cancellation_applied"}



    # E. RENEWAL TERMINATED (Permanent failure)

    elif event == "subscription.not_renewed":

        sub_code = data.get("subscription_code")

        await db.execute(

            update(MerchantPlan)

            .where(MerchantPlan.paystack_subscription_code == sub_code)

            .values(is_active=False, status="EXPIRED")

        )

        await db.commit()

        return {"status": "subscription_terminated"}



    return {"status": "ignored"}

    paystack_signature = request.headers.get("x-paystack-signature")



    if not paystack_signature:

        raise HTTPException(status_code=401, detail="Missing signature")



    computed_hmac = hmac.new(

        settings.PAYSTACK_SECRET_KEY.encode("utf-8"), raw_payload, hashlib.sha512

    ).hexdigest()



    if not hmac.compare_digest(computed_hmac, paystack_signature):

        logger.error("Invalid webhook signature detected!")

        raise HTTPException(status_code=401, detail="Invalid signature")



    # 2. Extract Data

    payload = await request.json()

    event = payload.get("event")

    data = payload.get("data", {})



    # --- HANDLE SOFT CANCELLATION ---

    if event == "subscription.disable":

        if event == "invoice.payment_failed":

            sub_code = data.get("subscription_code")

            # Mark the plan as PAST_DUE so the UI can show a 'Payment Failed' banner

            await db.execute(

                update(MerchantPlan)

                .where(MerchantPlan.paystack_subscription_code == sub_code)

                .values(status="PAST_DUE", is_active=False)

            )

            await db.commit()

            logger.warning(f"Upgrade payment failed for subscription {sub_code}")

        sub_code = data.get("subscription_code")

        # Soft Cancel: Keep is_active=True so they can use the remainder of their cycle.

        # Dashboard status changes to CANCELLING to inform the merchant.

        await db.execute(

            update(MerchantPlan)

            .where(MerchantPlan.paystack_subscription_code == sub_code)

            .values(status="CANCELLING")

        )

        await db.commit()

        logger.info(f"Subscription {sub_code} disabled - set to CANCELLING state.")

        return {"status": "soft_deactivated"}



    # --- HANDLE RECURRING RENEWALS & INITIAL SUCCESS ---

    if event == "invoice.payment_succeeded":

        reference = data.get("reference")

        subscription_data = data.get("subscription", {})

        sub_code = subscription_data.get("subscription_code") or data.get("subscription_code")

        amount_paid_kobo = data.get("amount", 0)



        stmt = select(MerchantPlan).where(MerchantPlan.paystack_subscription_code == sub_code)

        res = await db.execute(stmt)

        plan_record = res.scalar_one_or_none()



        if plan_record:

            # 1. Resolve what the plan SHOULD be. 

            new_paystack_plan_id = subscription_data.get("plan", {}).get("plan_code")



            # 2. Map Paystack PLN_... back to your internal STARTER/GROWTH/PRO using PlanService

            plan_def = PlanService.get_plan_by_paystack_id(new_paystack_plan_id)

            internal_plan_code = plan_def.plan_code if plan_def else plan_record.plan_code

            quota_messages = plan_def.quota_messages if plan_def else 0



            # 3. Update the record

            await db.execute(

                update(MerchantPlan)

                .where(MerchantPlan.tenant_id == plan_record.tenant_id)

                .values(

                    is_active=True,

                    status="ACTIVE",

                    plan_code=internal_plan_code,

                    quota_messages=quota_messages,

                    paystack_email_token=subscription_data.get("email_token"),

                    cycle_reset_at=datetime.fromisoformat(subscription_data.get("next_payment_date").replace("Z", "+00:00"))

                    if subscription_data.get("next_payment_date") else plan_record.cycle_reset_at

                )

            )

            await db.commit()

            logger.info(f"Plan updated via webhook for {sub_code}. Amount: {amount_paid_kobo}")



        # Update plan status

        if plan_record or invoice:

            tenant_id = plan_record.tenant_id if plan_record else invoice.tenant_id

            

            # Determine the "New Plan" state for the audit log

            # If it's a renewal, the plan code stays the same, but the status is confirmed ACTIVE

            current_plan_code = plan_record.plan_code if plan_record else metadata.get("plan_id")



            # Calculate next_cycle_date

            next_cycle_date = None

            if subscription_data.get("next_payment_date"):

                next_cycle_date = datetime.fromisoformat(subscription_data.get("next_payment_date").replace("Z", "+00:00"))

            else:

                next_cycle_date = datetime.utcnow() + timedelta(days=30)



            await db.execute(

                update(MerchantPlan)

                .where(MerchantPlan.tenant_id == tenant_id)

                .values(

                    is_active=True,

                    status="ACTIVE",

                    billing_cycle_start=datetime.utcnow(),

                    cycle_reset_at=next_cycle_date,

                    paystack_email_token=subscription_data.get("email_token"),

                )

            )

            await db.commit()



            # --- ADDED: UNIFIED AUDIT LOGGING ---

            background_tasks.add_task(

                AuditLogService.record_plan_change,

                tenant_id=tenant_id,

                actor_id=None,

                actor_type="SYSTEM",

                # For renewals, previous and new are the same, marking a 'RENEWAL' event

                previous_plan={"plan_id": current_plan_code, "status": "ACTIVE_OLD"},

                new_plan={"plan_id": current_plan_code, "status": "ACTIVE_RENEWED"},

                db=db,

            )



            logger.info(f"Subscription {sub_code} {event} processed for tenant {tenant_id}.")

            return {"status": "success"}



    # --- HANDLE INITIAL CHARGE (Success) ---

    if event == "charge.success":

        metadata = data.get("metadata", {})

        reference = data.get("reference")

        paystack_amount_kobo = data.get("amount")



        if not reference:

            return {"status": "ignored_no_reference"}



        stmt = select(MerchantInvoice).where(MerchantInvoice.id == UUID(reference))

        result = await db.execute(stmt)

        invoice = result.scalar_one_or_none()



        if not invoice or invoice.status == InvoiceStatus.PAID.value:

            return {"status": "skipped"}



        # --- UPDATED SECURITY CHECK ---

        expected_kobo = int(invoice.amount * 100)

        logger.info(

            f"Validating payment for invoice {invoice.id}: "

            f"Expected {expected_kobo} Kobo, Received {paystack_amount_kobo} Kobo"

        )

        if paystack_amount_kobo < expected_kobo:

            logger.error(

                "paystack_webhook_amount_mismatch",

                extra={

                    "invoice_id": str(invoice.id),

                    "expected_kobo": expected_kobo,

                    "received_kobo": paystack_amount_kobo,

                    "tenant_id": str(invoice.tenant_id),

                }

            )

            return {"status": "error_amount_mismatch", "detail": "Insufficient funds received"}



        # --- PROCEED WITH VALID ACTIVATION ---

        await InvoiceService.mark_invoice_paid(invoice_id=invoice.id, db=db)



        unpaid_invoice_ids = metadata.get("unpaid_invoice_ids", [])

        for inv_id in unpaid_invoice_ids:

            try:

                await InvoiceService.mark_invoice_paid(invoice_id=UUID(inv_id), db=db)

            except Exception:

                pass



        # Capture Sub Code, Token, and Activate Plan

        sub_data = data.get("subscription", {})

        sub_code = sub_data.get("subscription_code")

        email_token = sub_data.get("email_token")  # Capture the security token



        await db.execute(

            update(MerchantPlan)

            .where(MerchantPlan.tenant_id == invoice.tenant_id)

            .values(

                is_active=True,

                status="ACTIVE",

                paystack_subscription_code=sub_code,

                paystack_email_token=email_token,  # Save for future cancellation

                billing_cycle_start=datetime.utcnow(),

                # Use Paystack's next_payment_date if available

                cycle_reset_at=datetime.fromisoformat(sub_data.get("next_payment_date").replace("Z", "+00:00")) if sub_data.get("next_payment_date") else datetime.utcnow() + timedelta(days=30),

            )

        )

        await db.commit()



        background_tasks.add_task(

            AuditLogService.record_plan_change,

            tenant_id=invoice.tenant_id,

            actor_id=None,

            actor_type="SYSTEM",

            previous_plan=None,

            new_plan={"plan_id": metadata.get("plan_id"), "status": "ACTIVE"},

            db=db,

        )

        return {"status": "initial_success"}



    return {"status": "ignored"}





# --- Subscription Management ---





@router.post("/subscription/cancel")

async def cancel_subscription(

    db: AsyncSession = Depends(get_db_session),

    tenant_id: UUID = Depends(require_tenant_id),

    settings=Depends(get_settings),

):

    """Trigger programmatic cancellation of a Paystack subscription."""

    merchant_plan = await db.get(MerchantPlan, tenant_id)



    if not merchant_plan or not merchant_plan.paystack_subscription_code:

        raise HTTPException(

            status_code=400, detail="No active subscription found to cancel."

        )



    if not merchant_plan.paystack_email_token:

        raise HTTPException(

            status_code=400,

            detail="Subscription token missing. Please contact support.",

        )



    service = PaystackService(settings)



    try:

        # Request deactivation from Paystack.

        # Paystack will subsequently send a 'subscription.disable' webhook event.

        await service.cancel_subscription(

            subscription_code=merchant_plan.paystack_subscription_code,

            email_token=merchant_plan.paystack_email_token,

        )



        return {

            "message": "Cancellation request successful. Access remains active until the end of the period."

        }



    except PaystackServiceError as e:

        logger.error(f"Paystack cancellation error: {e}")

        raise HTTPException(

            status_code=500,

            detail="Failed to process cancellation with the payment provider.",

        )





@router.post("/subscription/checkout", response_model=SubscriptionCheckoutResponse)

async def subscription_checkout(

    payload: SubscriptionCheckoutRequest,

    db: AsyncSession = Depends(get_db_session),

    tenant_id: UUID = Depends(require_tenant_id),

    settings=Depends(get_settings),

):

    """Initiate transaction for selected plan or update existing for proration."""

    merchant_plan = await db.get(MerchantPlan, tenant_id)

    profile = await MerchantProfileService.get_profile_by_tenant_id(tenant_id, db)



    if not merchant_plan or merchant_plan.currency_lock != "NGN":

        raise HTTPException(

            status_code=409, detail="NGN currency required for Paystack checkout."

        )



    service = PaystackService(settings)



    # --- 1. HANDLE UPGRADE (Existing Subscription) ---

    if merchant_plan.paystack_subscription_code:

        plan_def = PlanService.get_plan_by_code(payload.plan_id)

        target_paystack_plan = plan_def.paystack_id if plan_def else None

        try:

            update_res = await service.update_subscription(

                subscription_code=merchant_plan.paystack_subscription_code,

                plan_code=target_paystack_plan,

            )

            data = update_res.get("data", {})

            # If Paystack generated a 'link' or an invoice for immediate payment:

            if data.get("invoice"):

                checkout_url = data["invoice"].get("hosted_url") or payload.success_url

                return SubscriptionCheckoutResponse(checkout_url=checkout_url)

            # If no immediate payment is required, return success

            return SubscriptionCheckoutResponse(checkout_url=payload.success_url)

        except PaystackServiceError as e:

            logger.error(f"Upgrade flow failed for tenant {tenant_id}: {e}")

            raise HTTPException(status_code=500, detail="Plan upgrade failed.")



    # 2. HANDLE NEW SUBSCRIPTION

    unpaid_invoices = await InvoiceService.get_unpaid_phone_invoices(

        tenant_id=tenant_id, db=db

    )



    new_invoice = await InvoiceService.create_pending_invoice(

        tenant_id=tenant_id,

        amount=merchant_plan.price,

        currency="NGN",

        category=InvoiceCategory.SUBSCRIPTION,

        description=None,

        db=db,

    )



    service = PaystackService(settings)

    intent = await service.initialize_subscription(

        email=profile.email,

        plan_code=merchant_plan.paystack_plan_id,

            amount=merchant_plan.price,  # Directly from the DB record

            reference=str(new_invoice.id),

            metadata={

                "plan_id": merchant_plan.plan_code,

                "unpaid_invoice_ids": [str(inv.id) for inv in unpaid_invoices],

                "tenant_id": str(tenant_id),

                "price": str(merchant_plan.price),  # Still included for dashboard visibility

            },

    )



    return SubscriptionCheckoutResponse(checkout_url=intent.authorization_url)





@router.post("/plan-intent", response_model=PlanIntentResponse)

async def apply_plan_intent(

    payload: PlanIntentRequest,

    db: AsyncSession = Depends(get_db_session),

    tenant_id=Depends(require_tenant_id),

    current_session: SessionInfo | None = Depends(get_current_session),

) -> PlanIntentResponse:

    """Record user's choice of plan before payment."""

    actor_type = "MERCHANT" if current_session else "SYSTEM"

    actor_id = current_session.user_id if current_session else None



    try:

        result: PlanIntentResult = await PlanIntentService.apply_plan_intent(

            tenant_id=tenant_id,

            plan=payload.plan,

            interval=payload.interval,

            actor_id=actor_id,

            actor_type=actor_type,

            db=db,

        )

        # Update onboarding state to record PLAN_SELECTION

        from src.services.onboarding_state_service import OnboardingStateService

        await OnboardingStateService.compute_state(tenant_id=tenant_id, db=db)

        return PlanIntentResponse(

            plan=result.plan_code,

            interval=result.interval,

            currency=result.currency,

            quota_messages=result.quota_messages,

            price=result.price,

            billing_cycle_start=result.billing_cycle_start,

        )

    except PlanCurrencyMismatchError as exc:

        raise HTTPException(

            status_code=409, detail={"error_code": "CURR_ERR", "message": str(exc)}

        )

    except (PlanIntervalUnsupportedError, PlanNotFoundError) as exc:

        raise HTTPException(

            status_code=400, detail={"error_code": "PLAN_ERR", "message": str(exc)}

        )

    except PlanPrerequisiteError as exc:

        raise HTTPException(

            status_code=400, detail={"error_code": "PRE_ERR", "message": str(exc)}

        )





@router.get("/history", response_model=list[BillingActivityItem])

async def get_billing_history(

    db: AsyncSession = Depends(get_db_session),

    tenant_id: UUID = Depends(require_tenant_id),

):

    """Returns a merged feed of invoices and plan subscription changes."""

    return await BillingHistoryService.get_activity_feed(tenant_id=tenant_id, db=db)

the code below is from constant.py file

PLAN_MAPPING = {

    "STARTER": "PLN_i6att5rfn5hypyl",

    "GROWTH": "PLN_8hp9tz0uyzjxvp7",

    "PRO": "PLN_uo1i43zue58y532"

}



# map Paystack back to your Quotas

REVERSE_PLAN_MAPPING = {v: k for k, v in PLAN_MAPPING.items()}



PLAN_QUOTAS = {

    "STARTER": 400,

    "GROWTH": 1500,

    "PRO": 6000

}

PLAN_MAPPING = {

    "STARTER": "PLN_i6att5rfn5hypyl",

    "GROWTH": "PLN_8hp9tz0uyzjxvp7",

    "PRO": "PLN_uo1i43zue58y532"

}



the code below is from plan_intent_service.py file

"""Service layer for applying merchant plan selections."""



from __future__ import annotations



from dataclasses import dataclass

from datetime import datetime, timezone

from typing import Final

from uuid import UUID



from sqlalchemy.ext.asyncio import AsyncSession

from sqlalchemy.future import select



from src.core.constants import PLAN_MAPPING

from src.core.logger import get_logger

from src.models.onboarding import MerchantPlan

from src.services.audit_log_service import AuditLogService



logger = get_logger(__name__)





class PlanIntentError(RuntimeError):

    """Base exception for plan intent failures."""





class PlanNotFoundError(PlanIntentError):

    """Raised when a referenced plan does not exist in the catalogue."""





class PlanIntervalUnsupportedError(PlanIntentError):

    """Raised when a plan does not support the requested billing interval."""





class PlanCurrencyMismatchError(PlanIntentError):

    """Raised when a plan is not available for the tenant's locked currency."""





class PlanPrerequisiteError(PlanIntentError):

    """Raised when plan provisioning prerequisites are not satisfied."""





@dataclass(frozen=True)

class PlanCatalogueEntry:

    """Represents the resolved configuration for a specific plan variant."""



    plan_code: str

    interval: str

    currency: str

    quota_messages: int

    price: int





@dataclass(frozen=True)

class PlanIntentResult:

    """Summarises the outcome of an applied plan selection."""



    plan_code: str

    interval: str

    currency: str

    quota_messages: int

    price: int

    billing_cycle_start: datetime





class PlanIntentService:

    """Handle the translation of plan selections into persisted state."""



    _PLAN_CATALOGUE: Final[dict[str, dict[str, dict[str, dict[str, int]]]]] = {

        "STARTER": {

            "monthly": {

                "NGN": {"quota_messages": 400, "price": 10000},

                "USD": {"quota_messages": 400, "price": 39},

            }

        },

        "GROWTH": {

            "monthly": {

                "NGN": {"quota_messages": 1500, "price": 25000},

                "USD": {"quota_messages": 1500, "price": 99},

            }

        },

        "PRO": {

            "monthly": {

                "NGN": {"quota_messages": 6000, "price": 60000},

                "USD": {"quota_messages": 6000, "price": 249},

            }

        },

    }



    @classmethod

    def _normalize_plan(cls, plan: str) -> str:

        value = plan.strip().upper()

        if not value:

            raise PlanNotFoundError("Plan code is required")

        return value



    @classmethod

    def _normalize_interval(cls, interval: str) -> str:

        value = interval.strip().lower()

        if not value:

            raise PlanIntervalUnsupportedError("Billing interval is required")

        if value != "monthly":

            raise PlanIntervalUnsupportedError("Only 'monthly' interval is supported")

        return value



    @classmethod

    def _resolve_plan(

        cls,

        *,

        plan: str,

        interval: str,

        currency: str,

    ) -> PlanCatalogueEntry:

        plan_key = cls._normalize_plan(plan)

        interval_key = cls._normalize_interval(interval)

        currency_key = currency.strip().upper()



        plan_entry = cls._PLAN_CATALOGUE.get(plan_key)

        if plan_entry is None:

            raise PlanNotFoundError(f"Unsupported plan '{plan}'")



        interval_entry = plan_entry.get(interval_key)

        if interval_entry is None:

            raise PlanIntervalUnsupportedError(

                f"Plan '{plan_key}' does not support '{interval}' billing"

            )



        try:

            plan_data = interval_entry[currency_key]

        except KeyError as exc:

            raise PlanCurrencyMismatchError(

                "Selected plan is not available for the tenant currency"

            ) from exc



        return PlanCatalogueEntry(

            plan_code=plan_key,

            interval=interval_key,

            currency=currency_key,

            quota_messages=plan_data["quota_messages"],

            price=plan_data["price"],

        )



    @staticmethod

    def _serialize_plan_state(plan: MerchantPlan | None) -> dict[str, object] | None:

        if plan is None:

            return None

        return {

            "plan_code": plan.plan_code,

            "billing_interval": plan.billing_interval,

            "billing_cycle_start": plan.billing_cycle_start.isoformat()

            if plan.billing_cycle_start

            else None,

            "quota_messages": plan.quota_messages,

            "currency_lock": plan.currency_lock,

        }



    @staticmethod

    def _now() -> datetime:

        return datetime.now(timezone.utc)



    @classmethod

    async def apply_plan_intent(

        cls,

        *,

        tenant_id: UUID,

        plan: str,

        interval: str,

        actor_id: UUID | None,

        actor_type: str,

        db: AsyncSession,

    ) -> PlanIntentResult:

        """Apply merchant plan choice and persist state."""

        # 1. Fetch tracked object

        plan_record = await db.get(MerchantPlan, tenant_id)

        if plan_record is None or plan_record.currency_lock is None:

            raise PlanPrerequisiteError(

                "Tenant currency lock must be established before plan selection"

            )



        # 2. Resolve plan details

        selection = cls._resolve_plan(

            plan=plan,

            interval=interval,

            currency=plan_record.currency_lock,

        )



        # 3. Capture previous state for audit log

        previous_state = cls._serialize_plan_state(plan_record)



        # 4. Map to Paystack PLN code

        paystack_id = PLAN_MAPPING.get(selection.plan_code)

        if not paystack_id:

            raise PlanNotFoundError(f"No Paystack mapping for {selection.plan_code}")



        # 5. Update object attributes directly

        plan_record.plan_code = selection.plan_code

        plan_record.billing_interval = selection.interval

        plan_record.quota_messages = selection.quota_messages

        plan_record.price = selection.price

        plan_record.billing_cycle_start = cls._now()

        plan_record.paystack_plan_id = paystack_id



        # --- SAFETY UPDATES ---

        # Ensure quota reset date is None until payment is confirmed via webhook

        plan_record.cycle_reset_at = None

        # Plan remains inactive until the Webhook receives charge.success

        plan_record.is_active = False



        try:

            # 6. Atomic commit of plan change and audit log

            await AuditLogService.record_plan_change(

                tenant_id=tenant_id,

                actor_id=actor_id,

                actor_type=actor_type,

                previous_plan=previous_state,

                new_plan=cls._serialize_plan_state(plan_record) or {},

                db=db,

            )

            await db.commit()

        except Exception as exc:

            await db.rollback()

            logger.exception(

                "plan_intent_failed",

                extra={"tenant_id": str(tenant_id), "plan": selection.plan_code},

            )

            raise



        await db.refresh(plan_record)



        logger.info(

            "plan_intent_applied",

            extra={

                "tenant_id": str(tenant_id),

                "plan_code": selection.plan_code,

                "interval": selection.interval,

            },

        )



        return PlanIntentResult(

            plan_code=selection.plan_code,

            interval=selection.interval,

            currency=selection.currency,

            quota_messages=selection.quota_messages,

            price=selection.price,

            billing_cycle_start=plan_record.billing_cycle_start,

        )

the code below is from billing.py file under schema folder

from __future__ import annotations



from datetime import datetime

from typing import Dict, List, Optional, Any



from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field, field_validator





# --- Subscription Checkout Schemas ---





class SubscriptionCheckoutRequest(BaseModel):

    """Request body for subscription checkout."""



    # Ensure plan_id is normalized to lowercase for PLAN_MAPPING consistency

    plan_id: str = Field(..., description="The plan code the tenant wishes to purchase")

    success_url: AnyHttpUrl = Field(

        ..., description="URL to redirect to after successful payment"

    )

    cancel_url: AnyHttpUrl = Field(

        ..., description="URL to redirect to if payment is cancelled"

    )



    @field_validator("plan_id")

    @classmethod

    def normalize_plan_id(cls, v: str) -> str:

        return v.strip().upper()





class SubscriptionCheckoutResponse(BaseModel):

    """Response payload for subscription checkout."""



    checkout_url: AnyHttpUrl = Field(

        ..., description="The Paystack checkout URL for the merchant"

    )





# --- Plan Intent Schemas ---





class PlanIntentRequest(BaseModel):

    """Request body for plan selection."""



    plan: str = Field(..., description="The plan code the tenant wishes to activate")

    interval: str = Field(..., description="Billing interval, e.g. monthly")



    @field_validator("plan")

    @classmethod

    def normalize_plan(cls, v: str) -> str:

        return v.strip().upper()





class PlanIntentResponse(BaseModel):

    """Response payload describing the applied plan."""



    model_config = ConfigDict(from_attributes=True)



    plan: str

    interval: str

    currency: str

    quota_messages: int

    price: int

    billing_cycle_start: datetime





# --- Billing Activity Item Schema ---





class BillingActivityItem(BaseModel):

    event_type: str  # e.g., "INVOICE_PAID", "PLAN_RENEWED", "PLAN_CHANGE"

    timestamp: datetime

    description: str

    amount: Optional[float] = None

    currency: Optional[str] = None

    metadata: Optional[Dict[str, Any]] = None





# --- Dashboard Analytics Schemas ---





class RevenuePoint(BaseModel):

    date: str

    amount: float





class DashboardOverview(BaseModel):

    total_revenue: float

    completed_orders: int

    revenue_over_time: List[RevenuePoint]

    dispatch_stats: Dict[str, int]

    payment_distribution: Dict[str, int]





# --- Paystack Webhook Schemas ---





class PaystackPlanSchema(BaseModel):

    plan_code: str  # The PLN_... code





class PaystackSubscriptionSchema(BaseModel):

    subscription_code: str

    email_token: str

    next_payment_date: Optional[str] = None

    plan: PaystackPlanSchema





class PaystackCustomerSchema(BaseModel):

    email: str

    customer_code: str





class PaystackWebhookData(BaseModel):

    id: int

    domain: str

    status: str

    reference: str

    amount: int

    metadata: Dict[str, Any]

    subscription: Optional[PaystackSubscriptionSchema] = None

    customer: Optional[PaystackCustomerSchema] = None





class PaystackEventSchema(BaseModel):

    event: str

    data: PaystackWebhookData

the code below is from is from billing.py file under model folder

"""Billing-related ORM models for merchant invoices."""



from __future__ import annotations



from decimal import Decimal

from enum import Enum

from uuid import UUID, uuid4



from sqlalchemy import ForeignKey, Numeric, String

from sqlalchemy.dialects.postgresql import JSONB, UUID as SAUUID

from sqlalchemy.orm import Mapped, mapped_column



from src.models.base import Base





class InvoiceStatus(str, Enum):

    """Lifecycle states for merchant invoices."""



    PENDING = "PENDING"

    PAID = "PAID"

    CANCELLED = "CANCELLED"





class InvoiceCategory(str, Enum):

    """Categories for merchant invoices."""



    PHONE_NUMBER = "PHONE_NUMBER"

    SUBSCRIPTION = "SUBSCRIPTION"





class MerchantInvoice(Base):

    """Represents a billable item that must be settled by a tenant."""



    __tablename__ = "merchant_invoices"



    id: Mapped[UUID] = mapped_column(

        SAUUID(as_uuid=True), primary_key=True, default=uuid4

    )

    tenant_id: Mapped[UUID] = mapped_column(

        SAUUID(as_uuid=True),

        ForeignKey("merchants.id", ondelete="CASCADE"),

        nullable=False,

    )

    amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)

    currency: Mapped[str] = mapped_column(String(3), nullable=False)

    status: Mapped[str] = mapped_column(

        String(16), default=InvoiceStatus.PENDING.value, nullable=False

    )

    category: Mapped[str] = mapped_column(String(32), nullable=False)

    description: Mapped[str | None] = mapped_column(String(255), nullable=True)

    external_reference: Mapped[str | None] = mapped_column(String(64), nullable=True)

    metadata_json: Mapped[dict[str, object] | None] = mapped_column(

        "metadata", JSONB, nullable=True, default=dict

    )



    def __repr__(self) -> str:

        return (

            "<MerchantInvoice(id={id}, tenant_id={tenant_id}, status={status}, "

            "category={category})>"

        ).format(

            id=self.id,

            tenant_id=self.tenant_id,

            status=self.status,

            category=self.category,

        )

the code below is from merchant_profile_service.py file

from __future__ import annotations



"""Service abstraction for managing merchant profile records."""



import re

from typing import cast

from uuid import UUID



from sqlalchemy import Select, select

from sqlalchemy.ext.asyncio import AsyncSession



from src.core.logger import get_logger

from src.core.settings import Settings

from src.models.onboarding import DEFAULT_PROFILE_DOCUMENT_TYPE, MerchantProfile



logger = get_logger(__name__)





class MerchantProfileService:

    """Encapsulates profile update semantics for a tenant."""



    @staticmethod

    async def get_profile_by_tenant_id(tenant_id: UUID, db: AsyncSession) -> MerchantProfile | None:

        """Fetch a merchant profile by tenant_id."""

        return await db.get(MerchantProfile, tenant_id)



    @staticmethod

    async def upsert_profile(

        *,

        tenant_id: UUID,

        business_name: str | None,

        business_type: str | None,

        business_registration_number: str | None,

        phone_number: str | None,

        email: str | None,

        business_address: str | None,

        document_type: str | None = None,

        document_file: str | None = None,

        db: AsyncSession,

        settings: Settings,

    ) -> MerchantProfile:

        """Create or update a merchant profile record in a tenant-scoped manner."""

        created = False

        profile = await db.get(MerchantProfile, tenant_id)



        # 1. Validation for New Profiles

        if profile is None:

            required_fields = {

                "business_name": business_name,

                "business_type": business_type,

                "phone_number": phone_number,

                "email": email,

                "business_address": business_address,

            }

            missing = [name for name, value in required_fields.items() if value is None]

            if missing:

                raise ValueError(

                    "Missing required profile fields: {fields}".format(

                        fields=", ".join(sorted(missing))

                    )

                )



            phone_value = cast(str, phone_number)

            MerchantProfileService._validate_phone_number(phone_value)

            await MerchantProfileService._ensure_unique_phone_number(

                phone_number=phone_value,

                tenant_id=tenant_id,

                db=db,

            )



            profile = MerchantProfile(

                tenant_id=tenant_id,

                business_name=cast(str, business_name),

                business_type=cast(str, business_type),

                phone_number=phone_value,

                email=cast(str, email),

                business_address=cast(str, business_address),

                document_type=document_type or DEFAULT_PROFILE_DOCUMENT_TYPE,

                live_enabled=False,

            )

            created = True



        # 2. Update remaining fields

        if business_name is not None:

            profile.business_name = business_name

        if business_type is not None:

            profile.business_type = business_type

        if business_registration_number is not None:

            profile.business_registration_number = business_registration_number

        if phone_number is not None:

            MerchantProfileService._validate_phone_number(phone_number)

            if profile.phone_number != phone_number:

                await MerchantProfileService._ensure_unique_phone_number(

                    phone_number=phone_number,

                    tenant_id=tenant_id,

                    db=db,

                )

            profile.phone_number = phone_number

        if email is not None:

            profile.email = email

        if business_address is not None:

            profile.business_address = business_address

        if document_type is not None:

            profile.document_type = document_type

        if document_file is not None:

            profile.document_file = document_file



        if created:

            db.add(profile)



        try:

            await db.commit()

        except Exception:

            await db.rollback()

            logger.exception(

                "merchant_profile_upsert_failed",

                extra={"tenant_id": str(tenant_id)},

            )

            raise



        logger.info(

            "merchant_profile_upserted",

            extra={

                "tenant_id": str(tenant_id),

                "profile_created": created,

            },

        )

        return profile



    @staticmethod

    def _validate_phone_number(phone_number: str) -> None:

        pattern = re.compile(r"^\+[1-9]\d{1,14}$")

        if not pattern.fullmatch(phone_number):

            raise ValueError("phone_number must be in E.164 format")



    @staticmethod

    async def _ensure_unique_phone_number(

        *, phone_number: str, tenant_id: UUID, db: AsyncSession

    ) -> None:

        stmt: Select[tuple[UUID]] = (

            select(MerchantProfile.tenant_id)

            .where(

                MerchantProfile.phone_number == phone_number,

                MerchantProfile.tenant_id != tenant_id,

            )

            .limit(1)

        )

        result = await db.execute(stmt)

        if result.scalar_one_or_none() is not None:

            raise ValueError("phone_number already in use")

the code below is from audit_log_service.py file

"""Service helpers for recording tenant audit events."""



from __future__ import annotations



from dataclasses import dataclass

from datetime import datetime

from typing import Sequence

from uuid import UUID



from sqlalchemy import Select, func, select

from sqlalchemy.ext.asyncio import AsyncSession



from src.core.logger import get_logger

from src.models.audit import TenantAuditLog



logger = get_logger(__name__)





class AuditLogService:

    """Provides helpers for persisting tenant audit log entries."""



    @staticmethod

    async def record_currency_lock_assignment(

        *, tenant_id: UUID, currency_code: str, phone_number: str, db: AsyncSession

    ) -> TenantAuditLog:

        """Persist an audit entry noting the assigned billing currency."""

        entry = TenantAuditLog(

            tenant_id=tenant_id,

            event_type="CURRENCY_LOCK_ASSIGNED",

            context={

                "currency": currency_code,

                "waba_phone": phone_number,

            },

        )

        db.add(entry)

        logger.info(

            "audit_currency_lock_assigned",

            extra={"tenant_id": str(tenant_id), "currency": currency_code},

        )

        return entry



    @staticmethod

    async def record_plan_change(

        *,

        tenant_id: UUID,

        actor_id: UUID | None,

        actor_type: str,

        previous_plan: dict[str, object] | None,

        new_plan: dict[str, object],

        db: AsyncSession,

    ) -> TenantAuditLog:

        """Persist an audit entry capturing a subscription plan change.

        If actor_id is None, treat as a system event.

        """

        context = {

            "actor_type": actor_type,

            "actor_id": str(actor_id) if actor_id is not None else None,

            "previous_plan": previous_plan,

            "new_plan": new_plan,

        }

        # If actor_id is None, mark as system event

        if actor_id is None:

            context["system_event"] = True

            context["actor_label"] = "System"

        else:

            context["system_event"] = False

            context["actor_label"] = str(actor_id)



        entry = TenantAuditLog(

            tenant_id=tenant_id,

            event_type="PLAN_CHANGED",

            context=context,

        )

        db.add(entry)

        logger.info(

            "audit_plan_changed",

            extra={

                "tenant_id": str(tenant_id),

                "actor_type": actor_type,

                "actor_id": str(actor_id) if actor_id is not None else "system",

            },

        )

        return entry



    @staticmethod

    async def record_phone_number_verified(

        *,

        tenant_id: UUID,

        phone_number: str,

        verification_sid: str | None,

        db: AsyncSession,

    ) -> TenantAuditLog:

        """Persist an audit entry noting successful phone verification."""



        entry = TenantAuditLog(

            tenant_id=tenant_id,

            event_type="PHONE_NUMBER_VERIFIED",

            context={

                "phone_number": phone_number,

                "verification_sid": verification_sid,

            },

        )

        db.add(entry)

        logger.info(

            "audit_phone_number_verified",

            extra={"tenant_id": str(tenant_id)},

        )

        return entry



    @staticmethod

    async def record_bank_account_linked(

        *,

        tenant_id: UUID,

        bank_code: str,

        bank_name: str | None,

        account_last4: str,

        subaccount_code: str,

        db: AsyncSession,

    ) -> TenantAuditLog:

        """Persist an audit entry noting that payout bank details were configured."""



        entry = TenantAuditLog(

            tenant_id=tenant_id,

            event_type="PAYOUT_ACCOUNT_LINKED",

            context={

                "bank_code": bank_code,

                "bank_name": bank_name,

                "account_last4": account_last4,

                "subaccount_code": subaccount_code,

            },

        )

        db.add(entry)

        logger.info(

            "audit_payout_account_linked",

            extra={

                "tenant_id": str(tenant_id),

                "bank_code": bank_code,

                "account_last4": account_last4,

            },

        )

        return entry



    @staticmethod

    async def record_merchant_go_live(

        *,

        tenant_id: UUID,

        merchant_email: str,

        merchant_name: str,

        db: AsyncSession,

    ) -> TenantAuditLog:

        """Persist an audit entry noting that a merchant completed go-live activation."""



        entry = TenantAuditLog(

            tenant_id=tenant_id,

            event_type="MERCHANT_LIVE_ACTIVATED",

            context={

                "merchant_email": merchant_email,

                "merchant_name": merchant_name,

            },

        )

        db.add(entry)

        logger.info(

            "audit_merchant_live_activated",

            extra={"tenant_id": str(tenant_id), "merchant_email": merchant_email},

        )

        return entry



    @dataclass(frozen=True)

    class PaginatedResult:

        """Container for paginated audit log results."""



        items: Sequence[TenantAuditLog]

        total: int

        page: int

        page_size: int



    @staticmethod

    async def list_logs(

        *,

        tenant_id: UUID,

        page: int,

        page_size: int,

        db: AsyncSession,

        event_type: str | None = None,

        created_from: datetime | None = None,

        created_to: datetime | None = None,

    ) -> "AuditLogService.PaginatedResult":

        """Return paginated audit log entries filtered for a tenant."""



        filters = [TenantAuditLog.tenant_id == tenant_id]

        if event_type:

            filters.append(TenantAuditLog.event_type == event_type)

        if created_from:

            filters.append(TenantAuditLog.created_at >= created_from)

        if created_to:

            filters.append(TenantAuditLog.created_at <= created_to)



        offset = (page - 1) * page_size



        base_select: Select[tuple[TenantAuditLog]] = (

            select(TenantAuditLog)

            .where(*filters)

            .order_by(TenantAuditLog.created_at.desc())

            .offset(offset)

            .limit(page_size)

        )



        total_select = select(func.count()).select_from(

            select(TenantAuditLog.id).where(*filters).subquery()

        )



        items_result = await db.scalars(base_select)

        total_result = await db.scalar(total_select)



        items = items_result.all()

        total = int(total_result or 0)



        logger.info(

            "audit_logs_listed",

            extra={

                "tenant_id": str(tenant_id),

                "page": page,

                "page_size": page_size,

                "filter_event_type": event_type,

            },

        )



        return AuditLogService.PaginatedResult(

            items=items,

            total=total,

            page=page,

            page_size=page_size,

        )



the code below is from plan_service.py file

from __future__ import annotations

from dataclasses import dataclass

from src.core.constants import PLAN_MAPPING, PLAN_QUOTAS



@dataclass(frozen=True)

class PlanDefinition:

    plan_code: str          # e.g., "PRO"

    paystack_id: str        # e.g., "PLN_uo1i43zue58y532"

    quota_messages: int     # e.g., 6000

    # You can add price here too if you want to centralize further



class PlanService:

    # Reverse mapping for quick lookups

    _REVERSE_MAPPING = {v: k for k, v in PLAN_MAPPING.items()}



    @classmethod

    def get_plan_by_paystack_id(cls, paystack_id: str) -> PlanDefinition | None:

        """Resolves a plan definition using a Paystack PLN_... code."""

        internal_code = cls._REVERSE_MAPPING.get(paystack_id)

        if not internal_code:

            return None

        

        return PlanDefinition(

            plan_code=internal_code,

            paystack_id=paystack_id,

            quota_messages=PLAN_QUOTAS.get(internal_code, 0)

        )



    @classmethod

    def get_plan_by_code(cls, plan_code: str) -> PlanDefinition | None:

        """Resolves a plan definition using an internal slug like 'PRO'."""

        paystack_id = PLAN_MAPPING.get(plan_code)

        if not paystack_id:

            return None



        return PlanDefinition(

            plan_code=plan_code,

            paystack_id=paystack_id,

            quota_messages=PLAN_QUOTAS.get(plan_code, 0)

        )



the code below is from billing.cleanup_service.py file

from datetime import datetime, timezone

from sqlalchemy import update

from src.models.onboarding import MerchantPlan

from src.core.logger import get_logger



logger = get_logger(__name__)



class BillingCleanupService:

    @staticmethod

    async def deactivate_expired_plans(db):

        """

        Hard deactivation for plans that have reached the end of their 

        pre-paid period after a cancellation request.

        """

        now = datetime.now(timezone.utc)



        # 1. Find plans that should be cut off

        # Condition: Status is CANCELLING and current time is past cycle_reset_at

        stmt = (

            update(MerchantPlan)

            .where(

                MerchantPlan.status == "CANCELLING",

                MerchantPlan.is_active == True,

                MerchantPlan.cycle_reset_at <= now

            )

            .values(

                is_active=False,

                status="EXPIRED"

            )

            .execution_options(synchronize_session="fetch")

        )



        result = await db.execute(stmt)

        await db.commit()



        count = result.rowcount

        if count > 0:

            logger.info(f"Billing Cleanup: Deactivated {count} expired merchant plans.")

        

        return count



the code below is from billing_cleanup_scheduler.py file

from fastapi_utils.tasks import repeat_every

from src.services.billing_cleanup_service import BillingCleanupService

from src.core.database import sessionmanager



# Scheduled task to deactivate expired subscriptions at midnight



def setup_billing_cleanup(app):

    @app.on_event("startup")

    @repeat_every(seconds=60 * 60 * 24)  # Runs every 24 hours

    async def scheduled_billing_cleanup():

        async with sessionmanager.session() as db:

            await BillingCleanupService.deactivate_expired_plans(db)



the code below is from billing_history_service.py file

from sqlalchemy.ext.asyncio import AsyncSession

from sqlalchemy import select

from uuid import UUID

from src.models.billing import MerchantInvoice

from src.models.audit import TenantAuditLog

from src.schemas.billing import BillingActivityItem



class BillingHistoryService:

    @staticmethod

    async def get_activity_feed(tenant_id: UUID, db: AsyncSession) -> list[BillingActivityItem]:

        # 1. Fetch Invoices

        invoice_stmt = select(MerchantInvoice).where(MerchantInvoice.tenant_id == tenant_id)

        invoice_res = await db.execute(invoice_stmt)

        invoices = invoice_res.scalars().all()



        # 2. Fetch Plan-related Audit Logs

        audit_stmt = select(TenantAuditLog).where(

            TenantAuditLog.tenant_id == tenant_id,

            TenantAuditLog.event_type == "PLAN_MANAGEMENT" # Filter for billing/plan events

        )

        audit_res = await db.execute(audit_stmt)

        logs = audit_res.scalars().all()



        feed: list[BillingActivityItem] = []



        # 3. Transform Invoices into Feed Items

        for inv in invoices:

            feed.append(BillingActivityItem(

                event_type=f"INVOICE_{inv.status}",

                timestamp=inv.created_at, # Or paid_at if you have it

                description=inv.description or f"Payment for {inv.category}",

                amount=float(inv.amount),

                currency=inv.currency,

                metadata={"invoice_id": str(inv.id)}

            ))



        # 4. Transform Audit Logs into Feed Items

        for log in logs:

            is_renewal = log.new_state.get("status") == "ACTIVE_RENEWED"

            feed.append(BillingActivityItem(

                event_type="PLAN_RENEWAL" if is_renewal else "PLAN_CHANGE",

                timestamp=log.created_at,

                description="Subscription automatically renewed" if is_renewal else "Subscription plan updated",

                metadata=log.new_state

            ))



        # 5. Sort by timestamp descending (newest first)

        feed.sort(key=lambda x: x.timestamp, reverse=True)

        return feed



the code below is from admin.py file

from fastapi import APIRouter, BackgroundTasks, Depends

from sqlalchemy.ext.asyncio import AsyncSession

from src.services.billing_cleanup_service import BillingCleanupService

from src.core.database import get_db_session



router = APIRouter(prefix="/admin", tags=["Admin"])



@router.post("/billing/force-cleanup", tags=["Admin"])

async def force_billing_cleanup(

    background_tasks: BackgroundTasks,

    db: AsyncSession = Depends(get_db_session),

    # TODO: Add admin-only authentication dependency

):

    """Manually trigger the expiration of cancelled subscriptions."""

    background_tasks.add_task(BillingCleanupService.deactivate_expired_plans, db)

    return {"message": "Cleanup task started in background."}



the code below is from main.py file

"""Main FastAPI application module for OryonTech Agent-001 API.



This module initializes the FastAPI application with middleware, exception handling,

and route configuration. It manages the application lifecycle (startup/shutdown).



Architecture:

    - Lifespan context manager handles startup and shutdown events

    - CORS middleware configured for cross-origin requests

    - Global exception handler catches and logs unhandled errors

    - API routes organized under /api/v1 prefix



Logging:

    All requests, errors, and lifecycle events are logged with context including:

    - Request method, path, and client IP

    - Error type and traceback

    - Application startup/shutdown timestamps

"""



from contextlib import asynccontextmanager

from datetime import datetime, timezone

import traceback



from fastapi import FastAPI

from fastapi.middleware.cors import CORSMiddleware

from fastapi.responses import JSONResponse

from fastapi import Request, status



from src.core.logger import get_logger

from src.core.settings import settings

from src.core.events import start_up, shut_down

from src.api.api import api_router

from src.core.billing_cleanup_scheduler import setup_billing_cleanup

from src.api.middleware.tenant_resolution import TenantResolutionMiddleware

from src.api.middleware.tracing import TracingMiddleware

from src.core.observability import setup_observability

from src.core.middleware import TenantModeMiddleware



logger = get_logger(__name__)





@asynccontextmanager

async def lifespan(app: FastAPI):

    """

    Application lifespan context manager.



    Handles application startup and shutdown with structured logging

    of initialization status and errors.

    """

    logger.info(

        "app_startup_initiated",

        extra={

            "app_name": settings.API_NAME,

            "version": settings.API_VERSION,

            "environment": settings.API_MODE,

            "timestamp": datetime.now(timezone.utc).isoformat(),

        },

    )



    try:

        setup_observability(app)

        await start_up()

        logger.info("app_startup_completed", extra={"status": "ready"})

        yield

    finally:

        logger.info(

            "app_shutdown_initiated",

            extra={"timestamp": datetime.now(timezone.utc).isoformat()},

        )

        await shut_down()

        logger.info("app_shutdown_completed", extra={"status": "stopped"})





"""Create and configure FastAPI application."""



app = FastAPI(

    lifespan=lifespan,

    title=settings.API_NAME,

    version=settings.API_VERSION,

    description=settings.API_DESCRIPTION,

    docs_url=settings.API_DOCS_URL,

    redoc_url="/redoc",

    openapi_url="/openapi.json",

)



# Setup scheduled billing cleanup task

setup_billing_cleanup(app)



app.add_middleware(

    CORSMiddleware,

    allow_origins=settings.CORS_ORIGINS,

    allow_credentials=True,

    allow_methods=["*"],

    allow_headers=settings.CORS_HEADERS,

)



app.add_middleware(TracingMiddleware)



app.add_middleware(TenantModeMiddleware)



app.add_middleware(

    TenantResolutionMiddleware,

    excluded_paths=(

        settings.API_DOCS_URL,

        "/openapi.json",

        "/redoc",

        "/favicon.ico",

        "/auth/magic-link",

        "/auth/callback",

        "/auth/session",

        "/auth/logout",

        f"{settings.API_V1_STR}/health",

        f"{settings.API_V1_STR}/auth/magic-link",

        f"{settings.API_V1_STR}/auth/callback",

        f"{settings.API_V1_STR}/auth/session",

        f"{settings.API_V1_STR}/auth/logout",

        f"{settings.API_V1_STR}/merchants",

        f"{settings.API_V1_STR}/onboarding/initiate",

        f"{settings.API_V1_STR}/onboarding/claim",

        f"{settings.API_V1_STR}/webhooks",

    ),

)





@app.exception_handler(Exception)

async def global_exception_handler(request: Request, exc: Exception):

    """

    Global exception handler for all unhandled exceptions.



    Logs exception with full context and returns 500 Internal Server Error.

    In production, sensitive error details are not exposed to the client.

    """

    client_ip = request.client.host if request.client else "unknown"



    logger.error(

        f"unhandled_exception: {type(exc).__name__}: {str(exc)}",

        extra={

            "error_type": type(exc).__name__,

            "error_message": str(exc),

            "path": request.url.path,

            "method": request.method,

            "client_ip": client_ip,

            "query_params": dict(request.query_params),

            "traceback": traceback.format_exc(),

            "timestamp": datetime.now(timezone.utc).isoformat(),

        },

    )



    return JSONResponse(

        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,

        content={"detail": "Internal server error"},

    )





app.include_router(api_router, prefix=settings.API_V1_STR)

