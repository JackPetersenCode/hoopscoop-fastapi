from fastapi import HTTPException
from helpers_and_columns.team_ids import allowed_teams

def construct_games_played_query(table_name, season, player_id, selected_opponent):
    # Construct the main SQL query
    games_played_bind_params = {}
    
    games_played_query = f"""
        SELECT COUNT(DISTINCT {table_name}.game_id) AS gp, {table_name}.player_id, {table_name}.team_id
        FROM {table_name}
        JOIN league_games_{season} 
        ON {table_name}.game_id = league_games_{season}.game_id
        AND {table_name}.team_id = league_games_{season}.team_id
        WHERE {table_name}.min > 0
    """

    # Conditionally add player_id filter
    if player_id != "1":
        games_played_bind_params['player_id'] = player_id
        games_played_query += f" AND {table_name}.player_id = :player_id"

    if selected_opponent != "1":
        games_played_bind_params['opponent_home'] = f'%vs. {selected_opponent}%'
        games_played_bind_params['opponent_visitor'] = f'%@ {selected_opponent}%'
        games_played_query += f" AND (league_games_{season}.matchup LIKE :opponent_home OR league_games_{season}.matchup LIKE :opponent_visitor)"
        
    games_played_query += f" GROUP BY {table_name}.player_id, {table_name}.team_id"
    
    return games_played_query, games_played_bind_params

    # Construct the query using the helper function
    # games_played_query, games_played_bind_params = construct_games_played_query(table_name, season, player_id, selected_opponent)
    # 
    # # Execute the query
    # games_played_query = text(games_played_query)       
    # games_played_query = games_played_query.bindparams(**games_played_bind_params)   
    # games_played_result = await session.execute(games_played_query)
    # 
    # games_played_columns = games_played_result.keys()
    # games_played_data = [dict(zip(games_played_columns, row)) for row in games_played_result.fetchall()]

#####################################################################################################3

def construct_player_stats_traditional_query(player_id, season, selected_team, selected_opponent):
        traditional_bind_params = {
            'selected_team_pattern': f'%{selected_team}%',
        }
       
        player_stats_traditional_query = f""" SELECT box_score_traditional_{season}.player_id,
			box_score_traditional_{season}.player_name,
            box_score_traditional_{season}.team_id,
            box_score_traditional_{season}.team_abbreviation,
            SUM(box_score_traditional_{season}.ast) AS ast,
            SUM(box_score_traditional_{season}.fgm) AS fgm,
            SUM(box_score_traditional_{season}.fg3a) AS fg3a,
            SUM(box_score_traditional_{season}.fg3m) AS fg3m,
            SUM(box_score_traditional_{season}.pts) AS pts,
            SUM(box_score_traditional_{season}.ftm) AS ftm,
            SUM(box_score_traditional_{season}.fta) AS fta,
            SUM(box_score_traditional_{season}.fga) AS fga,
            SUM(box_score_traditional_{season}.oreb) AS orb,
            SUM(box_score_traditional_{season}.dreb) AS drb,
            SUM(box_score_traditional_{season}.reb) AS reb,
            SUM(box_score_traditional_{season}.min) AS min,
            SUM(box_score_traditional_{season}.tov) AS tov,
            SUM(box_score_traditional_{season}.stl) AS stl,
            SUM(box_score_traditional_{season}.blk) AS blk,
            SUM(box_score_traditional_{season}.pf) AS pf
            FROM box_score_traditional_{season}
            JOIN league_games_{season}
            ON box_score_traditional_{season}.game_id = league_games_{season}.game_id
            AND box_score_traditional_{season}.team_id = league_games_{season}.team_id
            WHERE box_score_traditional_{season}.team_id LIKE :selected_team_pattern """
        if player_id != "1":
            traditional_bind_params['player_id'] = player_id
            player_stats_traditional_query += f" AND box_score_traditional_{season}.player_id = :player_id "

        # Conditionally add selected_opponent filter with parameterization
        if selected_opponent != "1":
            traditional_bind_params['opponent_home'] = f'%vs. {selected_opponent}%'
            traditional_bind_params['opponent_visitor'] = f'%@ {selected_opponent}%'
            player_stats_traditional_query += f" AND (league_games_{season}.matchup LIKE :opponent_home OR league_games_{season}.matchup LIKE :opponent_visitor) "

        # Conditionally add selected_team filter with parameterization
            
        player_stats_traditional_query += f""" GROUP BY box_score_traditional_{season}.player_id, 
            box_score_traditional_{season}.player_name, 
            box_score_traditional_{season}.team_id, 
            box_score_traditional_{season}.team_abbreviation 
            HAVING SUM(box_score_traditional_{season}.min) > 0
        """
       
        return player_stats_traditional_query, traditional_bind_params

        # player_stats_traditional_query, traditional_bind_params = construct_player_stats_traditional_query(player_id, season, selected_team, selected_opponent)
        # # Repeat similar process for other queries...
        # player_stats_traditional_query = text(player_stats_traditional_query)
        # player_stats_traditional_query = player_stats_traditional_query.bindparams(**traditional_bind_params)   
        # player_stats_traditional_result = await session.execute(player_stats_traditional_query)
        # player_stats_traditional_columns = player_stats_traditional_result.keys()
        # player_stats_traditional_data = [dict(zip(player_stats_traditional_columns, row)) for row in player_stats_traditional_result.fetchall()]

################################################################################################

def construct_player_stats_advanced_query(player_id, season, selected_team, selected_opponent):
        
        advanced_bind_params = {
            'selected_team_pattern': f'%{selected_team}%'
            # Add more parameters as needed
        }

        player_stats_advanced_query = f""" SELECT box_score_advanced_{season}.player_id,
		    box_score_advanced_{season}.player_name,
            box_score_advanced_{season}.team_id,
            box_score_advanced_{season}.team_abbreviation,
            ROUND(AVG(pie) * 100, 2) as pie,
            SUM(poss) as poss
            FROM box_score_advanced_{season}
            JOIN league_games_{season}
            ON box_score_advanced_{season}.game_id = league_games_{season}.game_id
            AND box_score_advanced_{season}.team_id = league_games_{season}.team_id
            WHERE box_score_advanced_{season}.team_id LIKE :selected_team_pattern """
        
        if player_id != "1":
            advanced_bind_params['player_id'] = player_id
            player_stats_advanced_query += f" AND box_score_advanced_{season}.player_id = :player_id "
        
        if selected_opponent != "1":
            advanced_bind_params['opponent_home'] = f'%vs. {selected_opponent}%'
            advanced_bind_params['opponent_visitor'] = f'%@ {selected_opponent}%'
            player_stats_advanced_query += f" AND (league_games_{season}.matchup LIKE :opponent_home OR league_games_{season}.matchup LIKE :opponent_visitor) "

        player_stats_advanced_query += f""" GROUP BY box_score_advanced_{season}.player_id, box_score_advanced_{season}.player_name, box_score_advanced_{season}.team_id, box_score_advanced_{season}.team_abbreviation
                                        HAVING SUM(box_score_advanced_{season}.min) > 0 """
        
        
        return player_stats_advanced_query, advanced_bind_params

def construct_team_stats_query(season, selected_team, selected_opponent):
                        
    team_stats_bind_params = {
        'selected_team_pattern': f'%{selected_team}%'
    }


    team_stats_query = f""" 
        SELECT box_score_traditional_{season}.team_id,
        box_score_traditional_{season}.team_abbreviation,
        SUM(box_score_traditional_{season}.fgm) AS fgm,
        SUM(box_score_traditional_{season}.fga) AS fga,
        SUM(box_score_traditional_{season}.fg3a) AS fg3a,
        SUM(box_score_traditional_{season}.fg3m) AS fg3m,
        SUM(box_score_traditional_{season}.ftm) AS ftm,
        SUM(box_score_traditional_{season}.fta) AS fta,
        SUM(box_score_traditional_{season}.tov) AS tov,
        SUM(box_score_traditional_{season}.oreb) AS orb,
        SUM(box_score_traditional_{season}.dreb) AS drb,
        SUM(box_score_traditional_{season}.reb) AS reb,
        SUM(box_score_traditional_{season}.pts) AS pts,
        SUM(box_score_traditional_{season}.min) AS min,
        SUM(box_score_traditional_{season}.ast) AS ast,
        SUM(box_score_traditional_{season}.blk) AS blk, 
        SUM(box_score_traditional_{season}.stl) AS stl,
        SUM(box_score_traditional_{season}.pf) AS pf
        FROM box_score_traditional_{season}
        JOIN league_games_{season}
        ON box_score_traditional_{season}.game_id = league_games_{season}.game_id
        AND box_score_traditional_{season}.team_id = league_games_{season}.team_id
        WHERE box_score_traditional_{season}.team_id LIKE :selected_team_pattern """
    
    if selected_opponent != "1":
        team_stats_bind_params['opponent_home'] = f'%vs. {selected_opponent}%'
        team_stats_bind_params['opponent_visitor'] = f'%@ {selected_opponent}%'
        team_stats_query += f"AND (league_games_{season}.matchup LIKE :opponent_home OR league_games_{season}.matchup LIKE :opponent_visitor) "
    
    team_stats_query += f"""GROUP BY box_score_traditional_{season}.team_id, box_score_traditional_{season}.team_abbreviation
              HAVING SUM(box_score_traditional_{season}.min) > 0
            ) """
    return team_stats_query, team_stats_bind_params 

def construct_traditional_totals_query(table_name, season, player_id, selected_opponent, selected_team, sort_field, order):
    # Start building the SQL query
    
    traditional_totals_bind_params = {
        'selected_team_pattern': f'%{selected_team}%',
        # Add more parameters as needed
    }
    
    traditional_totals_query = f"""
    SELECT 
        {table_name}.team_id, 
        {table_name}.team_abbreviation, 
        {table_name}.team_city, 
        player_id, 
        player_name, 
        SUM({table_name}.min) AS min, 
        SUM({table_name}.fgm) AS fgm, 
        SUM({table_name}.fga) AS fga, 
        SUM({table_name}.fgm) / NULLIF(SUM({table_name}.fga), 0) AS fg_pct, 
        SUM({table_name}.fg3m) AS fg3m, 
        SUM({table_name}.fg3a) AS fg3a, 
        SUM({table_name}.fg3m) / NULLIF(SUM({table_name}.fg3a), 0) AS fg3_pct, 
        SUM({table_name}.ftm) AS ftm, 
        SUM({table_name}.fta) AS fta, 
        SUM({table_name}.ftm) / NULLIF(SUM({table_name}.fta), 0) AS ft_pct, 
        SUM({table_name}.oreb) AS oreb, 
        SUM({table_name}.dreb) AS dreb, 
        SUM({table_name}.reb) AS reb, 
        SUM({table_name}.ast) AS ast, 
        SUM({table_name}.stl) AS stl, 
        SUM({table_name}.blk) AS blk, 
        SUM({table_name}.tov) AS tov, 
        SUM({table_name}.pf) AS pf, 
        SUM({table_name}.pts) AS pts, 
        SUM({table_name}.plus_minus) AS plus_minus 
    FROM 
        {table_name}
    JOIN league_games_{season}
    ON {table_name}.game_id = league_games_{season}.game_id
    AND {table_name}.team_id = league_games_{season}.team_id
    WHERE {table_name}.min > 0 """

    # Add player_id filter if necessary
    if player_id != "1":
        traditional_totals_bind_params['player_id'] = player_id
        traditional_totals_query += f"AND {table_name}.player_id = :player_id "

    # Add selected_opponent filter if necessary
    if selected_opponent != "1":
        traditional_totals_bind_params['opponent_home'] = f'%vs. {selected_opponent}%'
        traditional_totals_bind_params['opponent_visitor'] = f'%@ {selected_opponent}%'
        traditional_totals_bind_params['selected_opponent'] = selected_opponent
        traditional_totals_query += (
            f"AND (league_games_{season}.matchup LIKE :opponent_home "
            f"OR league_games_{season}.matchup LIKE :opponent_visitor) "
            f"AND {table_name}.team_abbreviation != :selected_opponent "
        )

    # Add selected_team filter
    traditional_totals_query += f"AND {table_name}.team_id LIKE :selected_team_pattern "

    # Final grouping and ordering
    traditional_totals_query += f"""
    GROUP BY player_id, player_name, {table_name}.team_id, 
             {table_name}.team_abbreviation, team_city 
    ORDER BY {sort_field} {order}
    """
    return traditional_totals_query, traditional_totals_bind_params

def construct_traditional_pergame_query(table_name, season, player_id, selected_opponent, selected_team, sort_field, order):

    traditional_pergame_bind_params = {
        'selected_team_pattern': f'%{selected_team}%'
        # Add more parameters as needed
    }
    
    games_played_query, games_played_bind_params = construct_games_played_query(table_name, season, player_id, selected_opponent)

    traditional_pergame_query = "WITH GamesPlayed AS ( " + games_played_query + " ) " + f"""
    SELECT
        {table_name}.team_id,
        {table_name}.team_abbreviation,
        team_city,
        {table_name}.player_id,
        player_name, 
        SUM({table_name}.min) / GamesPlayed.gp AS min,
        SUM({table_name}.fgm) / GamesPlayed.gp AS fgm,
        SUM({table_name}.fga) / GamesPlayed.gp AS fga,
        SUM({table_name}.fgm) / NULLIF(SUM({table_name}.fga), 0) AS fg_pct,
        SUM({table_name}.fg3m) / GamesPlayed.gp AS fg3m,
        SUM({table_name}.fg3a) / GamesPlayed.gp AS fg3a,
        SUM({table_name}.fg3m) / NULLIF(SUM({table_name}.fg3a), 0) AS fg3_pct,
        SUM({table_name}.ftm) / GamesPlayed.gp AS ftm,
        SUM({table_name}.fta) / GamesPlayed.gp AS fta,
        SUM({table_name}.ftm) / NULLIF(SUM({table_name}.fta), 0) AS ft_pct,
        SUM({table_name}.oreb) / GamesPlayed.gp AS oreb,
        SUM({table_name}.dreb) / GamesPlayed.gp AS dreb,
        SUM({table_name}.reb) / GamesPlayed.gp AS reb,
        SUM({table_name}.ast) / GamesPlayed.gp AS ast,
        SUM({table_name}.stl) / GamesPlayed.gp AS stl,
        SUM({table_name}.blk) / GamesPlayed.gp AS blk,
        SUM({table_name}.tov) / GamesPlayed.gp AS tov,
        SUM({table_name}.pf) / GamesPlayed.gp AS pf,
        SUM({table_name}.pts) / GamesPlayed.gp AS pts,
        SUM({table_name}.plus_minus) / GamesPlayed.gp AS plus_minus
    FROM
        {table_name}
    JOIN GamesPlayed
        ON {table_name}.player_id = GamesPlayed.player_id
        AND {table_name}.team_id = GamesPlayed.team_id
    JOIN league_games_{season}
        ON {table_name}.game_id = league_games_{season}.game_id
        AND {table_name}.team_id = league_games_{season}.team_id
    WHERE
        {table_name}.min > 0 """
    
    if player_id != "1":
        traditional_pergame_bind_params['player_id'] = player_id
        traditional_pergame_query += f"AND {table_name}.player_id = :player_id "

    if selected_opponent != "1":
        traditional_pergame_bind_params['opponent_home'] = f'%vs. {selected_opponent}%'
        traditional_pergame_bind_params['opponent_visitor'] = f'%@ {selected_opponent}%'
        traditional_pergame_bind_params['selected_opponent'] = selected_opponent        
        traditional_pergame_query += (f"AND (league_games_{season}.matchup LIKE :opponent_home "
                   f"OR league_games_{season}.matchup LIKE :opponent_visitor) "
                   f"AND {table_name}.team_abbreviation != :selected_opponent ")

    traditional_pergame_query += f"AND {table_name}.team_id LIKE :selected_team_pattern "
    traditional_pergame_query += f"""
    GROUP BY  
        {table_name}.player_id,
        {table_name}.player_name,
        {table_name}.team_id,
        {table_name}.team_abbreviation,
        team_city,
        GamesPlayed.gp
    ORDER BY {sort_field} {order}
    """

    return traditional_pergame_query, traditional_pergame_bind_params

def construct_traditional_perminute_query(table_name, season, player_id, selected_opponent, selected_team, sort_field, order, nMinutes):
    
    traditional_perminute_bind_params = {
        'selected_team_pattern': f'%{selected_team}%'
        # Add more parameters as needed
    }
    
    games_played_query, games_played_bind_params = construct_games_played_query(table_name, season, player_id, selected_opponent)
    traditional_perminute_query = "WITH GamesPlayed AS ( " + games_played_query + " ) " + f"""
                        SELECT
                            {table_name}.team_id, {table_name}.team_abbreviation, team_city,
                            {table_name}.player_id, player_name, 
                            SUM({table_name}.min) AS min,
                            {nMinutes} * (SUM({table_name}.fgm) / NULLIF(SUM({table_name}.min), 0)) AS fgm,
                            {nMinutes} * (SUM({table_name}.fga) / NULLIF(SUM({table_name}.min), 0)) AS fga,
                            SUM({table_name}.fgm) / NULLIF(SUM({table_name}.fga), 0) AS fg_pct,
                            {nMinutes} * (SUM({table_name}.fg3m) / NULLIF(SUM({table_name}.min), 0)) AS fg3m,
                            {nMinutes} * (SUM({table_name}.fg3a) / NULLIF(SUM({table_name}.min), 0)) AS fg3a,
                            SUM({table_name}.fg3m) / NULLIF(SUM({table_name}.fg3a), 0) AS fg3_pct,
                            {nMinutes} * (SUM({table_name}.ftm) / NULLIF(SUM({table_name}.min), 0)) AS ftm,
                            {nMinutes} * (SUM({table_name}.fta) / NULLIF(SUM({table_name}.min), 0)) AS fta,
                            SUM({table_name}.ftm) / NULLIF(SUM({table_name}.fta), 0) AS ft_pct,
                            {nMinutes} * (SUM({table_name}.oreb) / NULLIF(SUM({table_name}.min), 0)) AS oreb,
                            {nMinutes} * (SUM({table_name}.dreb) / NULLIF(SUM({table_name}.min), 0)) AS dreb,
                            {nMinutes} * (SUM({table_name}.reb) / NULLIF(SUM({table_name}.min), 0)) AS reb,
                            {nMinutes} * (SUM({table_name}.ast) / NULLIF(SUM({table_name}.min), 0)) AS ast,
                            {nMinutes} * (SUM({table_name}.stl) / NULLIF(SUM({table_name}.min), 0)) AS stl,
                            {nMinutes} * (SUM({table_name}.blk) / NULLIF(SUM({table_name}.min), 0)) AS blk,
                            {nMinutes} * (SUM({table_name}.tov) / NULLIF(SUM({table_name}.min), 0)) AS tov,
                            {nMinutes} * (SUM({table_name}.pf) / NULLIF(SUM({table_name}.min), 0)) AS pf,
                            {nMinutes} * (SUM({table_name}.pts) / NULLIF(SUM({table_name}.min), 0)) AS pts,
                            {nMinutes} * (SUM({table_name}.plus_minus) / NULLIF(SUM({table_name}.min), 0)) AS plus_minus
                        FROM
                            {table_name}
                        JOIN league_games_{season}
                            ON {table_name}.game_id = league_games_{season}.game_id
                            AND {table_name}.team_id = league_games_{season}.team_id
                        WHERE
                            {table_name}.min > 0
                        AND
                            {table_name}.team_id LIKE :selected_team_pattern """
    
    if player_id != "1":
        traditional_perminute_bind_params['player_id'] = player_id
        traditional_perminute_query += f"AND {table_name}.player_id = :player_id "

    if selected_opponent != "1":
        traditional_perminute_bind_params['opponent_home'] = f'%vs. {selected_opponent}%'
        traditional_perminute_bind_params['opponent_visitor'] = f'%@ {selected_opponent}%'
        traditional_perminute_bind_params['selected_opponent'] = selected_opponent        
        traditional_perminute_query += (f"AND (league_games_{season}.matchup LIKE :opponent_home "
                   f"OR league_games_{season}.matchup LIKE :opponent_visitor) "
                   f"AND {table_name}.team_abbreviation != :selected_opponent ")

    traditional_perminute_query += f"AND {table_name}.team_id LIKE :selected_team_pattern "
    traditional_perminute_query += f"""
    GROUP BY  
        {table_name}.player_id,
        {table_name}.player_name,
        {table_name}.team_id,
        {table_name}.team_abbreviation,
        team_city
    ORDER BY {sort_field} {order}
    """

    return traditional_perminute_query, traditional_perminute_bind_params

def construct_traditional_per100poss_query(table_name, season, player_id, selected_opponent, selected_team, sort_field, order):
    
    traditional_per100poss_bind_params = {
        'selected_team_pattern': f'%{selected_team}%'
        # Add more parameters as needed
    }
    
    games_played_query, games_played_bind_params = construct_games_played_query(table_name, season, player_id, selected_opponent)

    multiply_factor = 100
    joined_table = f"box_score_advanced_{season}"
    traditional_per100poss_query = "WITH GamesPlayed AS ( " + games_played_query + " ) " + f"""
                        SELECT
                            {table_name}.team_id, {table_name}.team_abbreviation, {table_name}.team_city,
                            {table_name}.player_id, {table_name}.player_name, 
                            {multiply_factor} * SUM({table_name}.min) / NULLIF(SUM({joined_table}.poss), 0) AS min,
                            {multiply_factor} * SUM({table_name}.fgm) / NULLIF(SUM({joined_table}.poss), 0) AS fgm,
                            {multiply_factor} * SUM({table_name}.fga) / NULLIF(SUM({joined_table}.poss), 0) AS fga,
                            SUM({table_name}.fgm) / NULLIF(SUM({table_name}.fga), 0) AS fg_pct,
                            {multiply_factor} * SUM({table_name}.fg3m) / NULLIF(SUM({joined_table}.poss), 0) AS fg3m,
                            {multiply_factor} * SUM({table_name}.fg3a) / NULLIF(SUM({joined_table}.poss), 0) AS fg3a,
                            SUM({table_name}.fg3m) / NULLIF(SUM({table_name}.fg3a), 0) AS fg3_pct,
                            {multiply_factor} * SUM({table_name}.ftm) / NULLIF(SUM({joined_table}.poss), 0) AS ftm,
                            {multiply_factor} * SUM({table_name}.fta) / NULLIF(SUM({joined_table}.poss), 0) AS fta,
                            SUM({table_name}.ftm) / NULLIF(SUM({table_name}.fta), 0) AS ft_pct,
                            {multiply_factor} * SUM({table_name}.oreb) / NULLIF(SUM({joined_table}.poss), 0) AS oreb,
                            {multiply_factor} * SUM({table_name}.dreb) / NULLIF(SUM({joined_table}.poss), 0) AS dreb,
                            {multiply_factor} * SUM({table_name}.reb) / NULLIF(SUM({joined_table}.poss), 0) AS reb,
                            {multiply_factor} * SUM({table_name}.ast) / NULLIF(SUM({joined_table}.poss), 0) AS ast,
                            {multiply_factor} * SUM({table_name}.stl) / NULLIF(SUM({joined_table}.poss), 0) AS stl,
                            {multiply_factor} * SUM({table_name}.blk) / NULLIF(SUM({joined_table}.poss), 0) AS blk,
                            {multiply_factor} * SUM({table_name}.tov) / NULLIF(SUM({joined_table}.poss), 0) AS tov,
                            {multiply_factor} * SUM({table_name}.pf) / NULLIF(SUM({joined_table}.poss), 0) AS pf,
                            {multiply_factor} * SUM({table_name}.pts) / NULLIF(SUM({joined_table}.poss), 0) AS pts,
                            {multiply_factor} * SUM({table_name}.plus_minus) / NULLIF(SUM({joined_table}.poss), 0) AS plus_minus
                        FROM
                            {table_name}
                        JOIN {joined_table}
                            ON {table_name}.player_id = {joined_table}.player_id
                            AND {table_name}.team_id = {joined_table}.team_id
                            AND {table_name}.game_id = {joined_table}.game_id
                        JOIN league_games_{season}
                            ON {table_name}.game_id = league_games_{season}.game_id
                            AND {table_name}.team_id = league_games_{season}.team_id
                        WHERE
                            {table_name}.min > 0
                        AND
                            {table_name}.team_id LIKE :selected_team_pattern """
    if player_id != "1":
        traditional_per100poss_bind_params['player_id'] = player_id
        traditional_per100poss_query += f"AND {table_name}.player_id = :player_id "

    if selected_opponent != "1":
        traditional_per100poss_bind_params['opponent_home'] = f'%vs. {selected_opponent}%'
        traditional_per100poss_bind_params['opponent_visitor'] = f'%@ {selected_opponent}%'
        traditional_per100poss_bind_params['selected_opponent'] = selected_opponent        
        traditional_per100poss_query += (f"AND (league_games_{season}.matchup LIKE :opponent_home "
                   f"OR league_games_{season}.matchup LIKE :opponent_visitor) "
                   f"AND {table_name}.team_abbreviation != :selected_opponent ")

    traditional_per100poss_query += f"""
    GROUP BY  
        {table_name}.player_id,
        {table_name}.player_name,
        {table_name}.team_id,
        {table_name}.team_abbreviation,
        {table_name}.team_city
    ORDER BY {sort_field} {order}
    """

    return traditional_per100poss_query, traditional_per100poss_bind_params

