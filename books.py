from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List

app = FastAPI()
BOOKS = [
    {"id": 1, "title": "1984", "author": "George Orwell", "year": 1949,'category': 'Dystopian Fiction'},
    {"id": 2, "title": "To Kill a Mockingbird", "author": "Harper Lee", "year": 1960,'category': 'Classic Fiction'},
    {"id": 3, "title": "The Great Gatsby", "author": "F. Scott Fitzgerald", "year": 1925,'category': 'Classic Fiction'},
    {"id": 4, "title": "The Catcher in the Rye", "author": "J.D. Salinger", "year": 1951,'category': 'Coming-of-Age Fiction'},
    {"id": 5, "title": "The Hobbit", "author": "J.R.R. Tolkien", "year": 1937,'category': 'Fantasy'},
    {"id": 6, "title": "The Da Vinci Code", "author": "Dan Brown", "year": 2003,'category': 'Mystery'},

]

@app.get("/")
async def first_api():
    return {"message": "This is the first API application."}


@app.get("/books")
async def get_books():
    return BOOKS

@app.get("/books/{dynamic_param}")
async def read_all_books(dynamic_param: str):
    return {'dynamic_param': dynamic_param,}