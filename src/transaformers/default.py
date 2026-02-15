from typing import Any, Dict, List, Optional, Union
import json
import pandas as pd
import numpy as np
from enum import Enum
import logging
from datetime import datetime

from src.schema.mapping import IndexInfo, FieldInfo
from src.core.backoff import backoff
from src.schema.errors import ValidationError
from src.abstracts.transform import AbstractTransform
from src.schema.obj import ObjList

logger = logging.getLogger(__name__)

class FilterOp(str, Enum):
    GT = "$gt"
    GE = "$ge"
    LT = "$lt"
    LE = "$le"
    EQUAL = "$equal"

class DataTransformer(AbstractTransform):
    def __init__(self, config):
        self._df_cache: Optional[pd.DataFrame] = None
        
    def transform(
        self, 
        index_config: IndexInfo, 
        batch_data: ObjList
    ) -> ObjList:
        """Основной метод трансформации батча данных"""
        if not batch_data:
            logger.debug("Пустой батч данных")
            return []
            
        try:
            df = self._prepare_dataframe(batch_data)
            df = self._apply_table_filter(df, index_config)
            result = self._transform_fields(df, index_config)
            
            logger.info(f"Трансформировано {len(result)} записей из {len(batch_data)}")
            return result
        except Exception as e:
            logger.error(f"Ошибка трансформации: {e}")
            return []

    def _prepare_dataframe(self, batch_data: ObjList) -> pd.DataFrame:
        """Создает DataFrame и применяет авто-конвертацию datetime"""
        df = pd.DataFrame(batch_data)
        self._auto_convert_datetime(df)
        return df

    def _auto_convert_datetime(self, df: pd.DataFrame) -> None:
        """✅ БЕЗОПАСНАЯ авто-конвертация datetime"""
        if df.empty:
            return
            
        for col in df.columns:
            if self._is_datetime_column(df, col):
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    logger.debug(f"Конвертирована колонка {col} в datetime")
                except Exception as e:
                    logger.warning(f"Не удалось конвертировать {col}: {e}")

    def _is_datetime_column(self, df: pd.DataFrame, col: str) -> bool:
        """✅ ИСПРАВЛЕНО - безопасная проверка datetime"""
        if df.empty or col not in df.columns:
            return 'time' in col.lower()
            
        try:
            if len(df) == 0:
                return False
            sample = str(df[col].iloc[0])
            patterns = ['T', '+00', '+', 'Z', '202[0-6]']
            return ('time' in col.lower() or any(pattern in sample for pattern in patterns))
        except:
            return 'time' in col.lower()

    def _apply_table_filter(
        self, 
        df: pd.DataFrame, 
        index_config: IndexInfo
    ) -> pd.DataFrame:
        """Применяет фильтр таблицы если указан"""
        table_options = index_config.get("options", {})
        if not table_options:
            return df
            
        try:
            filter_mask = self._evaluate_filter(df, table_options)
            filtered_df = df[filter_mask].reset_index(drop=True)
            logger.debug(f"Фильтр применил: {len(df)} -> {len(filtered_df)}")
            return filtered_df
        except Exception as e:
            logger.warning(f"Ошибка фильтрации: {e}")
            return df

    def _transform_fields(
        self, 
        df: pd.DataFrame, 
        index_config: IndexInfo
    ) -> List[Dict[str, Any]]:
        """✅ ИСПРАВЛЕНО - безопасная трансформация полей"""
        result_records = []
        fields_config = index_config["fields"]
        
        for idx in range(len(df)):
            if idx >= len(df):
                break
            row = df.iloc[idx]
            new_record = {}
            
            for new_field_name, field_info in fields_config.items():
                try:
                    options = field_info.get("options", {})
                    new_record[new_field_name] = self._process_field(df, row, idx, options)
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
        """✅ ИСПРАВЛЕНО - поддержка строк и списков"""
        try:
            if "$copy" in options:
                return self._copy_field(row, options["$copy"])
            elif "$to_json" in options:
                columns = options["$to_json"]
                if isinstance(columns, list):
                    return self._to_json_single(row, columns)
                else:
                    logger.warning(f"$to_json ожидает список, получено: {columns}")
                    return None
            elif "$max" in options:
                columns = options["$max"]
                if isinstance(columns, str):
                    columns = [columns]  # Строка → список
                return self._calculate_window_agg(df[columns], row_idx, "max")
            elif "$min" in options:
                columns = options["$min"]
                if isinstance(columns, str):
                    columns = [columns]  # ✅ Строка "price" → ["price"]
                return self._calculate_window_agg(df[columns], row_idx, "min")
            else:
                raise ValidationError(f"Неизвестный тип опции: {options}")
        except Exception as e:
            logger.error(f"Ошибка в _process_field: {e}")
            return None


    def _copy_field(self, row: pd.Series, column_name: str) -> Any:
        """Копирует поле из исходной строки"""
        if column_name not in row.index:
            logger.warning(f"Колонка {column_name} не найдена")
            return None
            
        value = row[column_name]
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
        data: Union[pd.DataFrame, pd.Series], 
        row_idx: int, 
        agg_type: str
    ) -> Any:
        """✅ ИСПРАВЛЕНО - принимает DataFrame И Series"""
        try:
            # Если DataFrame - берем первую колонку или агрегируем
            if isinstance(data, pd.DataFrame):
                if len(data.columns) == 1:
                    data = data.iloc[:, 0]  # Первая колонка → Series
                else:
                    # Несколько колонок - агрегация по батчу
                    if agg_type == "max":
                        return data.max(axis=1).iloc[row_idx] if len(data) > row_idx else None
                    elif agg_type == "min":
                        return data.min(axis=1).iloc[row_idx] if len(data) > row_idx else None

            # Теперь data гарантированно Series
            if isinstance(data, pd.Series) and len(data) > row_idx:
                if agg_type == "max":
                    return data.iloc[:row_idx+1].max() if row_idx >= 0 else data.max()
                elif agg_type == "min":
                    return data.iloc[:row_idx+1].min() if row_idx >= 0 else data.min()

            return None
        except Exception as e:
            logger.warning(f"Ошибка window_agg {agg_type}: {e}")
            return None

    def _serialize_value(self, value: Any) -> Any:
        """✅ ИСПРАВЛЕНО - безопасная сериализация"""
        if pd.isna(value) or value is None or value is np.nan:
            return None
        if hasattr(value, 'isoformat'):
            return value.isoformat()
        if isinstance(value, (pd.Timestamp, datetime)):
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
        """✅ ИСПРАВЛЕНО - финальная сериализация"""
        if pd.isna(value) or value is None or value is np.nan:
            return None
        elif isinstance(value, (pd.Timestamp, datetime)):
            return value.isoformat()
        elif isinstance(value, (np.integer, np.int64, np.int32)):  #type: ignore
            return int(value)
        elif isinstance(value, (np.floating, np.float64, np.float32)):  #type: ignore
            return float(value) if not pd.isna(value) else None
        elif isinstance(value, np.bool_):
            return bool(value)
        elif hasattr(value, '__str__'):
            return str(value)
        return value

    def _evaluate_filter(
        self, 
        df: pd.DataFrame, 
        filter_expr: Dict[str, Any]
    ) -> pd.Series:
        """✅ ИСПРАВЛЕНО - рекурсивная обработка фильтров"""
        if df.empty:
            return pd.Series([], dtype=bool)
            
        op = list(filter_expr.keys())[0]
        
        try:
            if op == "$and":
                sub_filters = filter_expr[op]
                masks = [self._evaluate_filter(df, cond) for cond in sub_filters]
                return np.logical_and.reduce(masks) if masks else pd.Series([True] * len(df), index=df.index)
                
            elif op == "$or":
                sub_filters = filter_expr[op]
                masks = [self._evaluate_filter(df, cond) for cond in sub_filters]
                return np.logical_or.reduce(masks) if masks else pd.Series([False] * len(df), index=df.index)
                
            elif op in [e.value for e in FilterOp]:
                return self._apply_simple_filter(df, filter_expr)
                
        except Exception as e:
            logger.warning(f"Ошибка фильтра {op}: {e}")
            
        raise ValidationError(f"Неизвестная операция фильтра: {op}")

    def _apply_simple_filter(
        self, 
        df: pd.DataFrame, 
        filter_cond: Dict[str, Any]
    ) -> pd.Series:
        """✅ ИСПРАВЛЕНО - простой фильтр"""
        if df.empty:
            return pd.Series([], dtype=bool)
            
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
        """✅ ИСПРАВЛЕНО - безопасная подготовка значения сравнения"""
        try:
            if pd.api.types.is_datetime64_any_dtype(series):
                result = pd.to_datetime(value, errors='coerce')
                if pd.isna(result):
                    return pd.NaT
                # ✅ Используем dtype.tz вместо series.dt.tz
                if series.dtype.tz is not None and result.tz is None:  #type: ignore
                    result = result.tz_localize('UTC')
                elif series.dtype.tz is not None and result.tz is not None:  #type: ignore
                    result = result.tz_convert('UTC')
                return result
            else:
                return pd.to_numeric(value, errors='coerce')
        except Exception as e:
            logger.warning(f"Ошибка подготовки значения сравнения: {e}")
            return pd.NaT if pd.api.types.is_datetime64_any_dtype(series) else np.nan

    def _apply_operator(
        self, 
        series: pd.Series, 
        op: str, 
        value: Any
    ) -> pd.Series:
        """✅ ИСПРАВЛЕНО - всегда возвращает Series"""
        try:
            if pd.isna(value):
                return pd.Series([False] * len(series), index=series.index)
            
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
        except Exception as e:
            logger.warning(f"Ошибка оператора {op}: {e}")
        
        return pd.Series([False] * len(series), index=series.index)
