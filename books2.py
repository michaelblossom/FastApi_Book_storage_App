from fastapi  import FastAPI, HTTPException,Body,Path,Query
from pydantic import BaseModel,Field
from typing import List, Optional
from starlette import status   
app = FastAPI()

class Book():
    id: int
    title: str
    author: str
    description: str
    rating: int
    published_date: int

    def __init__(self, id, title, author, description, rating, published_date):
        self.id = id
        self.title = title
        self.author = author
        self.description = description
        self.rating = rating
        self.published_date = published_date



class BookRequest(BaseModel):
    id:Optional[int] =Field(description='ID is not needed on create', default=None)
    title: str = Field(min_length=3)
    author: str = Field(min_length=1)
    description: str = Field(min_length=100)
    rating: int = Field(ge=1, le=5)
    published_date: int = Field(ge=0)

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "title": "The Alchemist",
                    "author": "Michael Blossom",
                    "description": "A philosophical book about a young shepherd's journey to find treasure and discover his personal legend.",
                    "rating": 5,
                    "published_date": 1988
                }
            ]
        }
    }




BOOKS = [
    Book(id=1, title="1984", author="George Orwell", description="Dystopian novel", rating=5, published_date=1949),
    Book(id=2, title="To Kill a Mockingbird", author="HarperLee", description="Classic novel", rating=5, published_date=1960),
    Book(id=3, title="The Great Gatsby", author="F. Scott Fitzgerald", description="Classic novel", rating=4, published_date=1925),
    Book(id=4, title="The Catcher in the Rye", author="J.D. Salinger", description="Coming-of-age novel", rating=4, published_date=1951),
    Book(id=5, title="The Hobbit", author="J.R.R. Tolkien", description="Fantasy novel", rating=5, published_date=1937),
    Book(id=6, title="The Da Vinci Code", author="Dan Brown", description="Mystery thriller", rating=4, published_date=2003),
]

@app.get("/books", status_code=status.HTTP_200_OK)
async def read_all_books():
    return BOOKS

@app.get("/books/{book_id}", status_code=status.HTTP_200_OK)
async def read_book(book_id: int = Path(gt=0)):
    for book in BOOKS:
        if book.id == book_id:
            return book
    raise HTTPException(status_code=404, detail="Book not found")

@app.get("/books/", status_code=status.HTTP_200_OK)
async def read_book_by_rating(rating: int = Query(ge=1, le=5)):
    result = []
    for book in BOOKS:
        if book.rating == rating:
            result.append(book)
    return result

@app.post("/books", status_code=status.HTTP_201_CREATED)
async def create_book(book: BookRequest ):
    new_book = Book(**book.dict())
    BOOKS.append(find_book_id(new_book))
    return new_book


def find_book_id(book:Book):
    if len(BOOKS) > 0:
        book.id = BOOKS[-1].id + 1
    else:
        book.id = 1
    return book

@app.put("/books/update_book",status_code=status.HTTP_204_NO_CONTENT)
async def update_book( book: BookRequest):
    for i in range(len(BOOKS)):
        if BOOKS[i].id == book.id:
            BOOKS[i] = Book(**book.dict())
            return BOOKS[i]


@app.delete("/books/{book_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_book(book_id: int = Path(gt=0)):
    for i in range(len(BOOKS)):
        if BOOKS[i].id == book_id:
            deleted_book = BOOKS.pop(i)
            return {"message": "Book deleted successfully", "book": deleted_book}