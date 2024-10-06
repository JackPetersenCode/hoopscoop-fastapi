def construct_offensive_query(season, player_id, selected_team, selected_opponent, selected_cte):
    bind_params = {
        'selected_team_pattern': f'%{selected_team}%'
    }
    offensive_rating = f""" 
      WITH PlayerStats AS (
        SELECT box_score_traditional_{season}.player_id,
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
        bind_params['player_id'] = player_id
        offensive_rating += f" AND box_score_traditional_{season}.player_id = :player_id "
    # Conditionally add selected_opponent filter with parameterization
    if selected_opponent != "1":
        bind_params['opponent_home'] = f'%vs. {selected_opponent}%'
        bind_params['opponent_visitor'] = f'%@ {selected_opponent}%'
        offensive_rating += f" AND (league_games_{season}.matchup LIKE :opponent_home OR league_games_{season}.matchup LIKE :opponent_visitor) "
    # Conditionally add selected_team filter with parameterization
            
    offensive_rating += f""" GROUP BY box_score_traditional_{season}.player_id, 
        box_score_traditional_{season}.player_name, 
        box_score_traditional_{season}.team_id, 
        box_score_traditional_{season}.team_abbreviation 
        HAVING SUM(box_score_traditional_{season}.min) > 0
        
    ),
    TeamStats AS (
        SELECT 
            box_score_traditional_{season}.team_id,
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
        offensive_rating += f" AND (league_games_{season}.matchup LIKE :opponent_home OR league_games_{season}.matchup LIKE :opponent_visitor) "
        
    offensive_rating += f"""GROUP BY box_score_traditional_{season}.team_id, 
        box_score_traditional_{season}.team_abbreviation
        HAVING SUM(box_score_traditional_{season}.min) > 0
    ),  
    Opponent_RB AS (
        SELECT
            t.team_abbreviation,
            t.team_id,
            SUM(CASE 
                    WHEN lg.matchup LIKE '%vs. ' || t.team_abbreviation || '%' 
                      OR lg.matchup LIKE '%@ ' || t.team_abbreviation || '%' 
                    THEN lg.reb 
                    ELSE 0 
                END) AS Opponent_TRB,
            SUM(CASE 
                    WHEN lg.matchup LIKE '%vs. ' || t.team_abbreviation || '%' 
                      OR lg.matchup LIKE '%@ ' || t.team_abbreviation || '%' 
                    THEN lg.oreb 
                    ELSE 0 
                END) AS Opponent_ORB
        FROM 
            (SELECT DISTINCT team_abbreviation, team_id
            FROM league_games_{season}) AS t
        JOIN league_games_{season} lg """
    if selected_opponent != "1":
        bind_params['opponent_home_orb'] = f'%vs. {selected_opponent}%'
        bind_params['opponent_visitor_orb'] = f'%@ {selected_opponent}%'
        offensive_rating += f"ON(lg.matchup LIKE :opponent_home_orb || t.team_abbreviation || '%' OR lg.matchup LIKE :opponent_visitor_orb || t.team_abbreviation || '%') " 
    else:
        offensive_rating += f"ON(lg.matchup LIKE '%vs. ' || t.team_abbreviation || '%' OR lg.matchup LIKE '%@ ' || t.team_abbreviation || '%') "
    offensive_rating += f"""GROUP BY t.team_abbreviation, t.team_id
    ),
    Team_Scoring_Poss AS(
    SELECT
      TeamStats.team_id,
        TeamStats.fgm + (1 - (1 - (TeamStats.ftm / TeamStats.fta)) ^ 2) * TeamStats.fta * 0.4 AS Team_Scoring_Poss
    
        FROM TeamStats
    ),
    Team_Play_PCT AS(
      SELECT Team_Scoring_Poss.team_id,
      Team_Scoring_Poss.Team_Scoring_Poss / (TeamStats.fga + TeamStats.fta * 0.4 + TeamStats.tov) AS Team_Play_PCT
    
        FROM Team_Scoring_Poss
      JOIN TeamStats
      ON Team_Scoring_Poss.team_id = TeamStats.team_id
    ),
    Team_ORB_PCT AS(
      SELECT TeamStats.team_id,
      TeamStats.orb / (TeamStats.orb + (Opponent_RB.Opponent_TRB - Opponent_RB.Opponent_ORB)) AS Team_ORB_PCT
      FROM TeamStats
      JOIN Opponent_RB
      ON TeamStats.team_id = Opponent_RB.team_id
    ),
    Team_ORB_Weight AS(
      SELECT Team_ORB_PCT.team_id,
      ((1 - Team_ORB_PCT.Team_ORB_PCT) * Team_Play_PCT.Team_Play_PCT) / ((1 - Team_ORB_PCT.Team_ORB_PCT) * Team_Play_PCT.Team_Play_PCT + Team_ORB_PCT.Team_ORB_PCT * (1 - Team_Play_PCT.Team_Play_PCT)) AS Team_ORB_Weight
    
        FROM Team_ORB_PCT
      JOIN Team_Play_PCT
      ON Team_ORB_PCT.team_id = Team_Play_PCT.team_id
    ),
    qAST AS(
      SELECT
      PlayerStats.player_id,
      PlayerStats.team_id,
      ((PlayerStats.min / NULLIF((TeamStats.min / 5), 0)) * (1.14 * NULLIF(((TeamStats.ast - PlayerStats.ast) / TeamStats.fgm), 0))) +
          ((((NULLIF((TeamStats.ast / TeamStats.min), 0)) * PlayerStats.min * 5 - PlayerStats.ast) /
          NULLIF(((TeamStats.fgm / TeamStats.min) * PlayerStats.min * 5 - PlayerStats.fgm), 0)) *
          (1 - (PlayerStats.min / NULLIF((TeamStats.min / 5), 0)))) AS qAST
      FROM
          PlayerStats
      JOIN TeamStats
      ON PlayerStats.team_id = TeamStats.team_id
      GROUP BY
      PlayerStats.player_id, PlayerStats.team_id, PlayerStats.min, TeamStats.min, PlayerStats.ast, TeamStats.ast, PlayerStats.fgm, TeamStats.fgm
    ),
    PProd_ORB_Part AS(
      SELECT
      PlayerStats.player_id,
      PlayerStats.team_id,
      PlayerStats.orb * Team_ORB_Weight.Team_ORB_Weight * Team_Play_PCT.Team_Play_PCT * (TeamStats.pts / (TeamStats.fgm + (1 - (1 - (TeamStats.ftm / TeamStats.fta)) ^ 2) * 0.4 * TeamStats.fta)) AS PProd_ORB_Part
      FROM PlayerStats
      JOIN Team_ORB_Weight
      ON PlayerStats.team_id = Team_ORB_Weight.team_id
      JOIN Team_Play_PCT
      ON PlayerStats.team_id = Team_Play_PCT.team_id
      JOIN TeamStats
      ON PlayerStats.team_id = TeamStats.team_id
      GROUP BY PlayerStats.player_id, PlayerStats.team_id, PlayerStats.orb, Team_ORB_Weight.Team_ORB_Weight, Team_Play_PCT.Team_Play_PCT, TeamStats.pts, TeamStats.fgm, TeamStats.ftm, TeamStats.fta
    ),
    PProd_FG_Part AS(
      SELECT
    
          PlayerStats.player_id,
      	PlayerStats.team_id,
        CASE
            WHEN PlayerStats.fga IS NULL OR PlayerStats.fga = 0 THEN 2 * (PlayerStats.fgm + 0.5 * PlayerStats.fg3m) * (1 - 0.5 * (0) * qAST.qAST)
    
            ELSE 2 * (PlayerStats.fgm + 0.5 * PlayerStats.fg3m) * (1 - 0.5 * ((PlayerStats.pts - PlayerStats.ftm) / (2 * PlayerStats.fga)) * qAST.qAST)
    
          END AS PProd_FG_Part
      FROM
        qAST
      JOIN PlayerStats ON qAST.player_id = PlayerStats.player_id AND qAST.team_id = PlayerStats.team_id
    
         GROUP BY PlayerStats.player_id, PlayerStats.team_id, PlayerStats.fga, PlayerStats.fgm, PlayerStats.fg3m, PlayerStats.pts, PlayerStats.ftm, qAST.qAST
    ),
    PProd_AST_Part AS(
      SELECT PlayerStats.player_id,
      PlayerStats.team_id,
      2 * ((TeamStats.fgm - PlayerStats.fgm + 0.5 * (TeamStats.fg3m - PlayerStats.fg3m)) / (TeamStats.fgm - PlayerStats.fgm)) * 0.5 * (((TeamStats.pts - TeamStats.ftm) - (PlayerStats.pts - PlayerStats.ftm)) / (2 * (TeamStats.fga - PlayerStats.fga))) * PlayerStats.ast AS PProd_AST_Part
    
        FROM PlayerStats
      JOIN TeamStats
      ON PlayerStats.team_id = TeamStats.team_id
      GROUP BY PlayerStats.player_id, PlayerStats.team_id, TeamStats.fgm, PlayerStats.fgm, TeamStats.fg3m, PlayerStats.fg3m, TeamStats.pts, TeamStats.ftm, PlayerStats.pts, PlayerStats.ftm, TeamStats.fga, PlayerStats.fga, PlayerStats.ast
    ),
    Parts AS(
        SELECT PProd_FG_Part.player_id, PProd_FG_Part.team_id, PProd_FG_Part.PProd_FG_Part AS FG_Part,
                    PProd_AST_Part.PProd_AST_Part AS AST_Part,
            PProd_ORB_Part.PProd_ORB_Part AS ORB_Part
        From PProd_FG_Part
        join PProd_AST_Part
        on PProd_FG_Part.player_id = PProd_AST_Part.player_id AND PProd_FG_Part.team_id = PProd_AST_Part.team_id
    
        join PProd_ORB_Part
    
        on PProd_FG_Part.player_id = PProd_ORB_Part.player_id AND PProd_FG_Part.team_id = PProd_ORB_Part.team_id
    ),
    PProd AS(
        SELECT Parts.player_id, Parts.team_id, (Parts.FG_Part +Parts.AST_Part + PlayerStats.FTM) *(1 - (TeamStats.orb / Team_Scoring_Poss.Team_Scoring_Poss) * Team_ORB_Weight.Team_ORB_Weight * Team_Play_PCT.Team_Play_PCT) + Parts.ORB_Part AS PProd
        FROM Parts
        JOIN PlayerStats
        ON Parts.player_id = PlayerStats.player_id
        AND Parts.team_id = PlayerStats.team_id
        JOIN TeamStats
        ON Parts.team_id = TeamStats.team_id
        JOIN Team_Scoring_Poss
        ON Parts.team_id = Team_Scoring_Poss.team_id
        JOIN Team_ORB_Weight
        ON Parts.team_id = Team_ORB_Weight.team_id
        JOIN Team_Play_PCT
        ON Parts.team_id = Team_Play_PCT.team_id
        GROUP BY Parts.player_id, Parts.team_id, Parts.FG_Part, Parts.AST_Part, PlayerStats.FTM, TeamStats.orb, Team_Scoring_Poss.Team_Scoring_Poss, Team_ORB_Weight.Team_ORB_Weight, Team_Play_PCT.Team_Play_PCT, Parts.ORB_Part
    ), 
    Poss_Parts AS(
      SELECT PlayerStats.player_id, PlayerStats.team_id,
      CASE
            WHEN PlayerStats.fta IS NULL OR PlayerStats.fta = 0 THEN 0
    
                ELSE(1 - (1 - (PlayerStats.ftm / PlayerStats.fta)) ^ 2) * 0.4 * PlayerStats.fta
      END AS Poss_FT_Part,
      CASE
          WHEN PlayerStats.fga IS NULL OR PlayerStats.fga = 0 THEN PlayerStats.fgm * (1 - 0.5 * (0) * qAST.qAST)
    
              ELSE PlayerStats.fgm * (1 - 0.5 * ((PlayerStats.pts - PlayerStats.ftm) / (2 * PlayerStats.fga)) * qAST.qAST)
    
      END AS Poss_FG_Part,
      0.5 * (((TeamStats.pts - TeamStats.ftm) - (PlayerStats.pts - PlayerStats.ftm)) / (2 * (TeamStats.fga - PlayerStats.fga))) * PlayerStats.ast AS Poss_AST_Part,
      PlayerStats.orb* Team_ORB_Weight.Team_ORB_Weight* Team_Play_PCT.Team_Play_PCT AS Poss_ORB_Part
      FROM PlayerStats
      JOIN qAST
      ON PlayerStats.player_id = qAST.player_id AND PlayerStats.team_id = qAST.team_id
      JOIN TeamStats
      ON PlayerStats.team_id = TeamStats.team_id
      JOIN Team_ORB_Weight
      ON PlayerStats.team_id = Team_ORB_Weight.team_id
      JOIN Team_Play_PCT
      ON PlayerStats.team_id = Team_Play_PCT.team_id
      GROUP BY PlayerStats.player_id, PlayerStats.team_id, PlayerStats.fta, PlayerStats.ftm, PlayerStats.fga, PlayerStats.fgm, PlayerStats.pts, qAST.qAST, TeamStats.pts, TeamStats.ftm, TeamStats.fga, PlayerStats.ast, PlayerStats.orb, Team_ORB_Weight.Team_ORB_Weight, Team_Play_PCT.Team_Play_PCT
    ),
    Scoring_Poss AS(
      SELECT Poss_Parts.player_id, Poss_Parts.team_id,
      (Poss_Parts.Poss_FG_Part +Poss_Parts.Poss_AST_Part + Poss_Parts.Poss_FT_Part) *(1 - (TeamStats.orb / Team_Scoring_Poss.Team_Scoring_Poss) * Team_ORB_Weight.Team_ORB_Weight * Team_Play_PCT.Team_Play_PCT) + Poss_Parts.Poss_ORB_Part AS Scoring_Poss
    
        FROM Poss_Parts
      JOIN TeamStats
      ON Poss_Parts.team_id = TeamStats.team_id
      JOIN Team_Scoring_Poss
      ON Poss_Parts.team_id = Team_Scoring_Poss.team_id
      JOIN Team_ORB_Weight
      ON Poss_Parts.team_id = Team_ORB_Weight.team_id
      JOIN Team_Play_PCT
      ON Poss_Parts.team_id = Team_Play_PCT.team_id
      GROUP BY Poss_Parts.player_id, Poss_Parts.team_id, Poss_Parts.Poss_FG_Part, Poss_Parts.Poss_AST_Part, Poss_Parts.Poss_FT_Part, TeamStats.ORB, Team_Scoring_Poss.Team_Scoring_Poss, Team_ORB_Weight.Team_ORB_Weight, Team_Play_PCT.Team_Play_PCT, Poss_Parts.Poss_ORB_Part
    ),
    xPoss AS(
        SELECT PlayerStats.player_id, PlayerStats.team_id,
        (PlayerStats.fga -PlayerStats.fgm) *(1 - 1.07 * Team_ORB_PCT.Team_ORB_PCT) AS FGxPoss,
        CASE
    
            WHEN PlayerStats.fta IS NULL OR PlayerStats.fta = 0 THEN 0
    
                ELSE((1 - (PlayerStats.ftm / PlayerStats.fta)) ^ 2) * 0.4 * PlayerStats.fta
        END AS FTxPoss
      FROM PlayerStats
      JOIN Team_ORB_PCT
      ON PlayerStats.team_id = Team_ORB_PCT.team_id
    ),
    Total_Poss AS(
        SELECT Scoring_Poss.player_id, Scoring_Poss.team_id,
        (Scoring_Poss.Scoring_Poss +xPoss.FGxPoss + xPoss.FTxPoss + PlayerStats.tov) AS Total_Poss
    
        FROM Scoring_Poss
      JOIN xPoss
      ON Scoring_Poss.player_id = xPoss.player_id AND Scoring_Poss.team_id = xPoss.team_id
      JOIN PlayerStats
      ON Scoring_Poss.player_id = PlayerStats.player_id AND Scoring_Poss.team_id = PlayerStats.team_id
    ),
    Offensive_Rating AS (
    SELECT PProd.player_id, PProd.team_id,
      CASE
          WHEN Total_Poss.Total_Poss IS NULL OR Total_Poss.Total_Poss = 0 THEN 0
          ELSE 100 * (PProd.PProd / Total_Poss.Total_Poss)
      END AS Offensive_Rating
      FROM PProd
      JOIN Total_Poss
      ON PProd.player_id = Total_Poss.player_id AND PProd.team_id = Total_Poss.team_id
    )
    """
    offensive_rating += f"SELECT * FROM {selected_cte}"

    return offensive_rating, bind_params
