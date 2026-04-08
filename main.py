"""
Основной модуль API для доступа к результатам скрапинга.
Реализовано с использованием FastAPI.
Содержит эндпоинты для получения данных с фильтрацией, сортировкой и пагинацией.
"""
import os
from typing import Optional, List, Dict, Any
from fastapi import FastAPI, HTTPException, Query
import pandas as pd

app = FastAPI(
    title="Scraping Results API",
    description="API для доступа к данным, собранным скрапером. Поддерживает фильтрацию и пагинацию.",
    version="1.0.0"
)

DATA_PATH = os.path.join(os.path.dirname(__file__), "data", "sample_data.csv")


def load_data() -> pd.DataFrame:
    """Вспомогательная функция для загрузки данных из CSV."""
    if not os.path.exists(DATA_PATH):
        raise HTTPException(status_code=500, detail="База данных (CSV) не найдена.")
    try:
        return pd.read_csv(DATA_PATH)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка чтения данных: {str(e)}")


@app.get("/")
def root() -> Dict[str, str]:
    """Корневой эндпоинт с приветственным сообщением."""
    return {
        "message": "Добро пожаловать в API скрапера!",
        "docs_url": "/docs",
        "instruction": "Перейдите на /docs для просмотра интерактивной документации."
    }


@app.get("/api/v1/books/", response_model=List[Dict[str, Any]])
def get_books(
    skip: int = Query(0, ge=0, description="Сколько записей пропустить (offset)"),
    limit: int = Query(10, ge=1, le=50, description="Максимальное количество записей (не более 50 из соображений трафика)"),
    min_price: Optional[float] = Query(None, ge=0, description="Минимальная цена"),
    max_price: Optional[float] = Query(None, ge=0, description="Максимальная цена"),
    availability: Optional[str] = Query(None, description="Статус наличия (например, 'In stock')"),
    sort_by: Optional[str] = Query(None, description="Поле для сортировки (price, title, timestamp)"),
    sort_desc: bool = Query(False, description="Сортировать по убыванию (True/False)")
) -> Any:
    """
    Получение среза данных с поддержкой фильтрации, сортировки и пагинации.
    """
    df = load_data()

    if min_price is not None:
        df = df[df['price'] >= min_price]
    
    if max_price is not None:
        df = df[df['price'] <= max_price]
        
    if availability is not None:
        df = df[df['availability'].str.lower() == availability.lower()]

    if df.empty:
        return []

    valid_sort_columns = ['price', 'title', 'timestamp']
    if sort_by:
        if sort_by not in valid_sort_columns:
            raise HTTPException(status_code=400, detail=f"Недопустимое поле для сортировки. Разрешены: {valid_sort_columns}")
        df = df.sort_values(by=sort_by, ascending=not sort_desc)

    df_slice = df.iloc[skip : skip + limit]

    df_slice = df_slice.where(pd.notnull(df_slice), None)
    return df_slice.to_dict(orient="records")