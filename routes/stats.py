from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from helpers_and_columns.columns import stat_columns
from database import async_session
from typing import Optional
import re
from helpers_and_columns.helpers import combine_stats, execute_query, validate_parameters
from sql.offensive_rating_query_functions import construct_offensive_query
from sql.query_construction_functions import construct_games_played_query, construct_player_stats_traditional_query, construct_player_stats_advanced_query, construct_traditional_per100poss_query, construct_traditional_pergame_query, construct_traditional_perminute_query, construct_traditional_totals_query
from helpers_and_columns.team_ids import allowed_teams
router = APIRouter()

async def get_async_session():
    async with async_session() as session:
        yield session

@router.get('/api/stats/')
async def get_stats(
    season: str = None,
    game_id: Optional[str] = None,
    order: str = "ASC",
    box_type: str = "Traditional",
    sort_field: str = "min",
    per_mode: str = "Totals",
    selected_team: str = "1",
    selected_opponent: str = "1",
    player_id: str = "1",
    stats: str = None,
    session: AsyncSession = Depends(get_async_session)
):
    print("Entering get_stats")

    try:
        print(season)
        print(selected_team)
        print(selected_opponent)
        print(player_id)
        print(box_type)
        print(order)
        print(sort_field)
        if box_type == 'Base':
            box_type = 'Traditional'

        
        validated = validate_parameters(season, box_type, selected_team, sort_field, order, player_id, selected_opponent, per_mode)
        if validated != True:
            return "Parameters were not validated"

        # Construct table name dynamically
        table_name = f'box_score_{box_type.lower()}_{season}'

        print(table_name)

        if stats is not None:

            print('STATS IS NOT NONE')
            stats_bind_params = {
            }


            stat_list = stats.split(',')
            print(stat_list)
            # for each stat, add to an array, then join together.
            STAT_TABLE_MAPPING = {
                'pts': {
                    'table': f"box_score_traditional_{season}",
                    'statement': 'PlayerStats.pts',
                    ''
                    'aggregate': 'SUM',
                },
                'ast': 'box_score_traditional',
                'reb': 'box_score_traditional',
                # Add more mappings as needed
            }

            offensive_cte_stats = [
                'PlayerStats',
                'TeamStats',
                'Opponent_RB',
                'Team_Scoring_Poss',
                'Team_Play_PCT',
                'Team_ORB_PCT',
                'Team_ORB_Weight',
                'qAST',
                'PProd_ORB_Part',
                'PProd_FG_Part',
                'PProd_AST_Part',
                'Parts',
                'PProd',
                'Poss_Parts',
                'Scoring_Poss',
                'xPoss',
                'Total_Poss',
                'Offensive_Rating'
            ]
            # I want a comma-separated list of stats as a parameter
            # Return desired columns from different tables
            # Step One: Match stat parameters to box_types, or to list of stats with no box_type
            # Step Two: Make separate database calls for each box_type
            select_stats_query = []
            join_tables = []
            results = []
            for stat in stat_list:
                print(stat)
                if stat in offensive_cte_stats:
                    print(f'IN OFFENSIVE CTE STATS: {stat}')
                    query, bind_params = construct_offensive_query(season, player_id, selected_team, selected_opponent, stat)
                    stat_result = await execute_query(query, bind_params, session)
                    results.append(stat_result)
                else:
                    name = STAT_TABLE_MAPPING[stat].statement
                    table = STAT_TABLE_MAPPING[stat].table
    
                    print(name)
                    print(table)
    
                    select_stats_query.append(name)
                    join_tables.append(table)
    
                    table_name = STAT_TABLE_MAPPING[stat] + f"_{season}"
                    # Construct the SQL query dynamically based on the stat and table_name
                    query = f"""SELECT player_name, player_id, team_abbreviation, team_id, 
                                
                                SUM({stat}) AS {stat} 
                                FROM {table_name} 
                                GROUP BY player_name, player_id, team_abbreviation, team_id;"""  
                    stat_result = await execute_query(query, stats_bind_params, session)
                    # Append this stat result to the results list
                    results.append(stat_result)

            combined_results = combine_stats(results)

            return combined_results
        
        else:    
            if box_type == 'Traditional':
                if per_mode == 'Totals':
                    
                    traditional_totals_query, traditional_totals_bind_params = construct_traditional_totals_query(table_name, season, player_id, selected_opponent, selected_team, sort_field, order)
                    print(traditional_totals_query)
    
                    # Repeat similar process for other queries...
                    traditional_totals = await execute_query(traditional_totals_query, traditional_totals_bind_params, session)
                    return traditional_totals  # Return appropriate data based on your application logic
                
                elif per_mode == 'Per Game':
                    print('perrrr game')
                    traditional_pergame_query, traditional_pergame_bind_params = construct_traditional_pergame_query(table_name, season, player_id, selected_opponent, selected_team, sort_field, order)
                    print(traditional_pergame_query)
    
                    traditional_pergame = await execute_query(traditional_pergame_query, traditional_pergame_bind_params, session)
                    return traditional_pergame
    
                elif per_mode in ['Per Minute', 'Per 12 Minutes', 'Per 24 Minutes']:
                # Code block for 'Per Minute', 'Per 12 Minutes', or 'Per 24 Minutes'
    
                    nMinutes = 1
                    if per_mode == 'Per 12 Minutes':
                        nMinutes = 12
                    elif per_mode == 'Per 24 Minutes':
                        nMinutes = 24
                    print(per_mode)
                    print('per minute')
                    traditional_perminute_query, traditional_perminute_bind_params = construct_traditional_perminute_query(table_name, season, player_id, selected_opponent, selected_team, sort_field, order, nMinutes)
    
                    traditional_perminute = await execute_query(traditional_perminute_query, traditional_perminute_bind_params, session)
                    return traditional_perminute
        
                elif per_mode == 'Per 100 Poss':
                    print('perrrr 100')
                    traditional_per100poss_query, traditional_per100poss_bind_params = construct_traditional_per100poss_query(table_name, season, player_id, selected_opponent, selected_team, sort_field, order)
                    traditional_per100poss = await execute_query(traditional_per100poss_query, traditional_per100poss_bind_params, session)
                    return traditional_per100poss
            
            elif box_type == 'Advanced':
                
                        query = f"WITH " + offRatingQuery + ", " + defRatingQuery + ", " + advancedStats + f"""
                        SELECT
                            Advanced_Stats.team_id, Advanced_Stats.team_abbreviation, 
                            Advanced_Stats.player_id, Advanced_Stats.player_name,
                            Advanced_Stats.min,
                            ROUND(Offensive_Rating.Offensive_Rating, 2) AS off_rating,
                            ROUND(Defensive_Rating.Defensive_Rating, 2) AS def_rating,
                            ROUND(Offensive_Rating.Offensive_Rating - Defensive_Rating.Defensive_Rating, 2) AS net_rating,
                            Advanced_Stats.Ast_Pct, 
                            Advanced_Stats.Ast_Tov,
                            Advanced_Stats.Ast_Ratio,
                            Advanced_Stats.Oreb_Pct,
                            Advanced_Stats.Dreb_Pct,
                            Advanced_Stats.Reb_Pct,
                            Advanced_Stats.Tov_Pct,
                            Advanced_Stats.Efg_Pct,
                            Advanced_Stats.Ts_Pct,
                            Advanced_Stats.Usg_Pct,
                            PlayerStatsAdvanced.pie AS Pie,
                            PlayerStatsAdvanced.poss AS Poss
                            FROM Advanced_Stats
                            JOIN Offensive_Rating
                            ON Advanced_Stats.player_id = Offensive_Rating.player_id AND Advanced_Stats.team_id = Offensive_Rating.team_id
                            JOIN Defensive_Rating
                            ON Advanced_Stats.player_id = Defensive_Rating.player_id AND Advanced_Stats.team_id = Defensive_Rating.team_id
                            JOIN PlayerStatsAdvanced
                            ON Advanced_Stats.player_id = PlayerStatsAdvanced.player_id AND Advanced_Stats.team_id = PlayerStatsAdvanced.team_id
                            GROUP BY Advanced_Stats.player_id, Advanced_Stats.player_name, Advanced_Stats.team_id, Advanced_Stats.team_abbreviation, Advanced_Stats.min, 
                            Offensive_Rating.Offensive_Rating, Defensive_Rating.Defensive_Rating, Advanced_Stats.Ast_Pct, Advanced_Stats.Ast_Tov, Advanced_Stats.Ast_Ratio,
                            Advanced_Stats.Oreb_Pct, Advanced_Stats.Dreb_Pct, Advanced_Stats.Reb_Pct, Advanced_Stats.Tov_Pct, Advanced_Stats.Efg_Pct, Advanced_Stats.Ts_Pct,
                            Advanced_Stats.Usg_Pct, PlayerStatsAdvanced.Pie, PlayerStatsAdvanced.Poss
                            HAVING Advanced_Stats.min > 0
                        """
                
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))