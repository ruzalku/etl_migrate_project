from typing import Any, Dict, List, Optional
import json
import pandas as pd
import numpy as np
from enum import Enum
import logging
from datetime import datetime

from src.schema.mapping import IndexInfo, FieldInfo
from src.core.backoff import backoff
from src.schema.errors import ValidationError

logger = logging.getLogger(__name__)


class FilterOp(str, Enum):
    GT = "$gt"
    GE = "$ge"
    LT = "$lt"
    LE = "$le"
    EQUAL = "$equal"


class DataTransformer:
    """Трансформер данных PostgreSQL -> MongoDB"""
    
    def __init__(self):
        self._df_cache: Optional[pd.DataFrame] = None
        
    def transform(
        self, 
        index_config: IndexInfo, 
        batch_data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Основной метод трансформации батча данных"""
        if not batch_data:
            logger.debug("Пустой батч данных")
            return []
            
        df = self._prepare_dataframe(batch_data)
        df = self._apply_table_filter(df, index_config)
        result = self._transform_fields(df, index_config)
        
        logger.info(f"Трансформировано {len(result)} записей из {len(batch_data)}")
        return result

    def _prepare_dataframe(self, batch_data: List[Dict[str, Any]]) -> pd.DataFrame:
        """Создает DataFrame и применяет авто-конвертацию datetime"""
        df = pd.DataFrame(batch_data)
        self._auto_convert_datetime(df)
        return df

    def _auto_convert_datetime(self, df: pd.DataFrame) -> None:
        """Автоматически конвертирует колонки в datetime"""
        for col in df.columns:
            if self._is_datetime_column(df, col):
                df[col] = pd.to_datetime(df[col], errors='coerce')
                logger.debug(f"Конвертирована колонка {col} в datetime")

    def _is_datetime_column(self, df: pd.DataFrame, col: str) -> bool:
        """Определяет, является ли колонка datetime"""
        sample = str(df[col].iloc[0]) if len(df) > 0 else ""
        patterns = ['T', '+00', '-20', '-21', '-22', '-23', '-24', '-25', '-26']
        return ('time' in col.lower() or 
                any(pattern in sample for pattern in patterns))

    def _apply_table_filter(
        self, 
        df: pd.DataFrame, 
        index_config: IndexInfo
    ) -> pd.DataFrame:
        """Применяет фильтр таблицы если указан"""
        table_options = index_config.get("options", {})
        if not table_options:
            return df
            
        filter_mask = self._evaluate_filter(df, table_options)
        filtered_df = df[filter_mask].reset_index(drop=True)
        logger.debug(f"Фильтр применил: {len(df)} -> {len(filtered_df)}")
        return filtered_df

    def _transform_fields(
        self, 
        df: pd.DataFrame, 
        index_config: IndexInfo
    ) -> List[Dict[str, Any]]:
        """Трансформирует все поля согласно конфигурации"""
        result_records = []
        fields_config = index_config["fields"]
        
        for idx, row in df.iterrows():
            new_record = {}
            for new_field_name, field_info in fields_config.items():
                try:
                    options = field_info.get("options", {})
                    new_record[new_field_name] = self._process_field(df, row, idx, options) #type: ignore
                except Exception as e:
                    logger.warning(f"Ошибка обработки поля {new_field_name}: {e}")
                    new_record[new_field_name] = None
                    
            result_records.append(self._make_json_serializable(new_record))
            
        return result_records

    def _process_field(
        self, 
        df: pd.DataFrame, 
        row: pd.Series, 
        row_idx: int, 
        options: Dict[str, Any]
    ) -> Any:
        """Обрабатывает одно поле согласно опциям"""
        if "$copy" in options:
            return self._copy_field(row, options["$copy"])
        elif "$to_json" in options:
            return self._to_json_single(row, options["$to_json"])
        elif "$max" in options:
            return self._calculate_window_agg(df[options["$max"]], row_idx, "max")
        elif "$min" in options:
            return self._calculate_window_agg(df[options["$min"]], row_idx, "min")
        else:
            raise ValidationError(f"Неизвестный тип опции: {options}")

    def _copy_field(self, row: pd.Series, column_name: str) -> Any:
        """Копирует поле из исходной строки"""
        if column_name not in row.index:
            logger.warning(f"Колонка {column_name} не найдена")
            return None
            
        value = row[column_name]
        if pd.isna(value):
            return None
            
        return self._serialize_value(value)

    def _to_json_single(self, row: pd.Series, columns: List[str]) -> str:
        """Создает JSON из указанных колонок строки"""
        data = {}
        for col in columns:
            if col in row.index:
                data[col] = self._serialize_value(row[col])
            else:
                data[col] = None
        return json.dumps(data)

    def _calculate_window_agg(
        self, 
        series: pd.Series, 
        row_idx: int, 
        agg_type: str
    ) -> Any:
        """Вычисляет агрегат для строки (заглушка для оконных функций)"""
        if agg_type == "max":
            return series.iloc[row_idx:].max()
        elif agg_type == "min":
            return series.iloc[row_idx:].min()
        return None

    def _serialize_value(self, value: Any) -> Any:
        """Сериализует значение для JSON"""
        if pd.isna(value):
            return None
        if hasattr(value, 'isoformat'):
            return value.isoformat()
        return str(value)

    @staticmethod
    def _make_json_serializable(record: Dict[str, Any]) -> Dict[str, Any]:
        """Финальная сериализация для MongoDB"""
        safe_record = {}
        for key, value in record.items():
            safe_record[key] = DataTransformer._serialize_final(value)
        return safe_record

    @staticmethod
    def _serialize_final(value: Any) -> Any:
        """Финальная проверка сериализации"""
        if pd.isna(value) or value is np.nan:
            return None
        elif isinstance(value, (pd.Timestamp, datetime)):
            return value.isoformat()
        elif isinstance(value, (np.integer, np.int64, np.int32)): #type: ignore
            return int(value)
        elif isinstance(value, (np.floating, np.float64, np.float32)): #type: ignore
            return float(value) if not pd.isna(value) else None
        elif isinstance(value, np.bool_):
            return bool(value)
        return value

    def _evaluate_filter(
        self, 
        df: pd.DataFrame, 
        filter_expr: Dict[str, Any]
    ) -> pd.Series:
        """Рекурсивная обработка фильтров"""
        op = list(filter_expr.keys())[0]
        
        if op == "$and":
            sub_filters = filter_expr[op]
            masks = [self._evaluate_filter(df, cond) for cond in sub_filters]
            return np.logical_and.reduce(masks) if masks else pd.Series([True] * len(df))
            
        elif op == "$or":
            sub_filters = filter_expr[op]
            masks = [self._evaluate_filter(df, cond) for cond in sub_filters]
            return np.logical_or.reduce(masks) if masks else pd.Series([False] * len(df))
            
        elif op in [e.value for e in FilterOp]:
            return self._apply_simple_filter(df, filter_expr)
            
        raise ValidationError(f"Неизвестная операция фильтра: {op}")

    def _apply_simple_filter(
        self, 
        df: pd.DataFrame, 
        filter_cond: Dict[str, Any]
    ) -> pd.Series:
        """Применяет простой фильтр к колонке"""
        op = list(filter_cond.keys())[0]
        cond_data = filter_cond[op]
        column = cond_data["column_name"]
        value = cond_data["value"]
        
        if column not in df.columns:
            logger.warning(f"Колонка {column} не найдена для фильтра")
            return pd.Series([False] * len(df), index=df.index)
        
        col_series = df[column]
        compare_value = self._prepare_compare_value(col_series, value)
        
        return self._apply_operator(col_series, op, compare_value)

    def _prepare_compare_value(self, series: pd.Series, value: Any) -> Any:
        """Подготавливает значение для сравнения"""
        if pd.api.types.is_datetime64_any_dtype(series):
            return pd.to_datetime(value, errors='coerce')
        return pd.to_numeric(value, errors='coerce')

    def _apply_operator(
        self, 
        series: pd.Series, 
        op: str, 
        value: Any
    ) -> pd.Series:
        """Применяет оператор сравнения"""
        if op == "$gt":
            return series > value
        elif op == "$ge":
            return series >= value
        elif op == "$lt":
            return series < value
        elif op == "$le":
            return series <= value
        elif op == "$equal":
            return series == value
        return pd.Series([False] * len(series), index=series.index)
