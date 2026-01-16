from typing import Annotated
from sqlalchemy.orm import Session
from database import SessionLocal
from fastapi import FastAPI,APIRouter, Depends, HTTPException, status
from starlette import status
from pydantic import BaseModel

from models import Users

# packages installed for authentication
from passlib.context import CryptContext

from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
router = APIRouter()

bcrypt_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

class CreateUserRequest(BaseModel):
    username: str
    password: str
    email: str
    first_name: str
    last_name: str
    role: str

# creating database dependency in
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

db_dependency = Annotated[Session, Depends(get_db)]


def authenticate_user(username: str, password: str, db):
    user = db.query(Users).filter(Users.username == username).first()
    if not user:
        return False
    if not bcrypt_context.verify(password, user.hashed_password):
        return False
    return user
    

@router.post("/auth/", status_code=status.HTTP_201_CREATED)
async def create(db: db_dependency, create_user_request: CreateUserRequest):
    create_user_model = Users(
        username=create_user_request.username,
        hashed_password=bcrypt_context.hash(create_user_request.password),
        email=create_user_request.email,
        first_name=create_user_request.first_name,
        last_name=create_user_request.last_name,
        role=create_user_request.role,
        is_active=True
    )
    db.add(create_user_model)
    db.commit()
    
    return create_user_model

@router.post("/token", status_code=status.HTTP_200_OK)
async def login_for_access_token(form_data:Annotated[OAuth2PasswordRequestForm, Depends()], db: db_dependency):
    user = authenticate_user(form_data.username, form_data.password, db)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return form_data.username

