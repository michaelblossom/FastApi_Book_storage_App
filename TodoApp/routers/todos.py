from typing import List, Annotated

from sqlalchemy.orm import Session

from pydantic import BaseModel, Field
from fastapi import FastAPI,APIRouter,Depends, HTTPException,Path,Query
from starlette import status

from models import Todos
from database import engine,SessionLocal


router = APIRouter()


# creating database dependency in
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

db_dependency = Annotated[Session, Depends(get_db)]

class TodoRequest(BaseModel):
    title: str = Field(min_length=3)
    description: str = Field(min_length=13, max_length=50)
    priority: int = Field(ge=1, le=5)
    duration: int = Field(ge=1)
    completed: bool = False

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "title": "List of provisions to buy",
                    "description": "Milk, Tea, biscuit, golden-morn,groundnut,garri",
                    "priority": 5,
                    "duration": 4,
                    "completed": False
                }
            ]
        }
    }

@router.get("/")
async def read_all(db: db_dependency):
    todos = db.query(Todos).all()
    return todos


@router.get("/todo/{todo_id}", status_code=status.HTTP_200_OK)
async def read_todo(db: db_dependency, todo_id: int = Path(gt=0)):
    todo = db.query(Todos).filter(Todos.id == todo_id).first()
    if not todo:
        raise HTTPException(status_code=404, detail=f"Todo with id {todo_id} not found")
    return todo


@router.post("/todo", status_code=status.HTTP_201_CREATED)
async def create_todo(db: db_dependency, todo_request: TodoRequest):
    todo = Todos(**todo_request.dict())
    db.add(todo)
    db.commit()
    db.refresh(todo)
    return todo
   
@router.patch("/todo/{todo_id}", status_code=status.HTTP_200_OK)
async def update_todo(db: db_dependency, todo_id: int = Path(gt=0), todo_request: TodoRequest = ...):
    todo = db.query(Todos).filter(Todos.id == todo_id).first()
    if not todo:
        raise HTTPException(status_code=404, detail=f"No Todo with id {todo_id} found")
    todo.title = todo_request.title
    todo.description = todo_request.description
    todo.priority = todo_request.priority
    todo.completed = todo_request.completed
    db.commit()
    db.refresh(todo)
    return todo


@router.delete("/todo/{todo_id}", status_code=status.HTTP_200_OK)
async def delete_todo(db: db_dependency, todo_id: int = Path(gt=0)):
    todo = db.query(Todos).filter(Todos.id == todo_id).first()
    if not todo:
        raise HTTPException(status_code=404, detail=f"Todo with id {todo_id} not found")
    db.delete(todo)
    db.commit()
    return {"detail": f"Todo with id {todo_id} has been deleted successfully"}