import re
from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from helpers_and_columns.team_ids import allowed_teams, allowed_box_types, allowed_seasons, allowed_opponents, allowed_per_modes
from helpers_and_columns.columns import stat_columns
from typing import List, Dict, Any


async def execute_query(query, params, session: AsyncSession):
    query = text(query)
    query = query.bindparams(**params)   
    result = await session.execute(query)
    columns = result.keys()
    data = [dict(zip(columns, row)) for row in result.fetchall()]
    return data

def validate_parameters(season, box_type, selected_team, sort_field, order, player_id, selected_opponent, per_mode):
    if season not in allowed_seasons:
        raise HTTPException(status_code=400, detail='Invalid season selected.')
    if selected_team not in allowed_teams:
        raise HTTPException(status_code=400, detail='Invalid team selected.')
    if box_type not in allowed_box_types:
        raise HTTPException(status_code=400, detail='Invalid box type.')
    if per_mode not in allowed_per_modes:
        raise HTTPException(status_code=400, detail='Invalid per mode.')

    # Validate sort_field to prevent SQL injection
    allowed_sort_fields = stat_columns[0][f"box_score_{box_type.lower()}_columns"]
    

    if sort_field not in allowed_sort_fields:
        raise HTTPException(status_code=400, detail='Invalid sort_field parameter.')
    # Validate order to prevent unintended behavior
    if order.upper() not in ['ASC', 'DESC']:
        raise HTTPException(status_code=400, detail='Invalid order parameter. Must be ASC or DESC.')
    if player_id is not None:
        if len(player_id) > 9:
            raise HTTPException(status_code=400, detail='Invalid player_id.')
    if selected_opponent not in allowed_opponents:
        raise HTTPException(status_code=400, detail='Invalid opponent.')
    else:
        return True

        
##New Code to combine the list of stats
def combine_stats(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    combined_stats = {}
    print('COMBINE STATS*****************************')
    for stat in results[0]:
        player_id = stat['player_id']
        team_id = stat['team_id']

        # Create a unique key based on player_id and team_id
        key = f"{player_id}-{team_id}"

        # Initialize if the key doesn't exist
        if key not in combined_stats:
            combined_stats[key] = {
                "player_id": player_id,
                "team_id": team_id,
                # Initialize an empty dictionary to hold all stats
                "stats": {}
            }

        # Add all stats that are not player_id or team_id
        for stat_name, stat_value in stat.items():
            if stat_name not in ['player_id', 'team_id']:
                combined_stats[key]['stats'][stat_name] = stat_value

    # Convert the combined_stats dictionary back to a list
    return list(combined_stats.values())