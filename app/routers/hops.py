from fastapi import APIRouter, HTTPException, Query
from typing import Optional, Dict, List
import pandas as pd
from sqlalchemy.sql import text
from app.db.connection_manager import ConnectionManager

router = APIRouter()
conn_mgr = ConnectionManager()

def get_hop(by_v_id: Optional[str] = None, by_v_label_en: Optional[str] = None, by_like_v_label_en: Optional[str] = None, limit: int = 50) -> pd.DataFrame:
    try:
        engine = conn_mgr.get_selected_engine()
        with engine.connect() as conn:
            if by_v_id:
                sql_query = f"""
                    SELECT TOP ({limit}) v_id, v_label_en, v_type, hop_next_v_id, hop_next_v_label_en, hop_next_v_type, e_id, e_type, hop_edge_direction
                    FROM graph.hop
                    WHERE v_id = :v_id
                """
                params = {'v_id': by_v_id}
            elif by_v_label_en:
                sql_query = f"""
                    SELECT TOP ({limit}) v_id, v_label_en, v_type, hop_next_v_id, hop_next_v_label_en, hop_next_v_type, e_id, e_type, hop_edge_direction
                    FROM graph.hop
                    WHERE v_label_en = :v_label_en
                """
                params = {'v_label_en': by_v_label_en}
            elif by_like_v_label_en:
                sql_query = f"""
                    SELECT TOP ({limit}) v_id, v_label_en, v_type, hop_next_v_id, hop_next_v_label_en, hop_next_v_type, e_id, e_type, hop_edge_direction
                    FROM graph.hop
                    WHERE v_label_en LIKE :by_like_v_label_en
                """
                params = {'by_like_v_label_en': f'%{by_like_v_label_en}%'}
            else:
                sql_query = f"""
                    SELECT TOP ({limit}) v_id, v_label_en, v_type, hop_next_v_id, hop_next_v_label_en, hop_next_v_type, e_id, e_type, hop_edge_direction
                    FROM graph.hop
                """
                params = {}

            return pd.read_sql(text(sql_query), con=conn, params=params)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def hop_to_cytoscape_json(df_hop: pd.DataFrame) -> Dict[str, List[Dict[str, Dict[str, str]]]]:
    df_nodes = pd.concat([
        df_hop[['v_id', 'v_label_en', 'v_type']].rename(columns={'v_id': 'id', 'v_label_en': 'label', 'v_type': 'type'}),
        df_hop[['hop_next_v_id', 'hop_next_v_label_en', 'hop_next_v_type']].rename(columns={'hop_next_v_id': 'id', 'hop_next_v_label_en': 'label', 'hop_next_v_type': 'type'})
    ]).drop_duplicates()

    df_edges = df_hop.drop_duplicates(subset=['v_id', 'hop_edge_direction', 'hop_next_v_id', 'e_id', 'e_type'])
    edge_data = pd.concat([
        df_edges[df_edges['hop_edge_direction'] == 'default'].rename(columns={'v_id': 'source', 'hop_next_v_id': 'target', 'e_id': 'id', 'e_type': 'type'}),
        df_edges[df_edges['hop_edge_direction'] == 'inverse'].rename(columns={'hop_next_v_id': 'source', 'v_id': 'target', 'e_id': 'id', 'e_type': 'type'})
    ])

    return {
        'nodes': [{'data': node.to_dict()} for _, node in df_nodes.iterrows()],
        'edges': [{'data': edge.to_dict()} for _, edge in edge_data.iterrows()]
    }

@router.get("/hops", response_model=Dict[str, List[Dict[str, Dict[str, str]]]])
async def read_hops(
    by_v_id: Optional[str] = Query(None),
    by_v_label_en: Optional[str] = Query(None),
    by_like_v_label_en: Optional[str] = Query(None),
    limit: int = Query(50, gt=0)
):
    df_hop = get_hop(by_v_id, by_v_label_en, by_like_v_label_en, limit)
    if df_hop is not None and not df_hop.empty:
        return hop_to_cytoscape_json(df_hop)
    else:
        raise HTTPException(status_code=404, detail="No data found")
