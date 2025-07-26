from fastapi import APIRouter

router = APIRouter()

@router.get("/")
def get_fondos():
    return {"fondos": []}
