SELECT u.name, jelly_colors.name as color
  FROM jelly_daily_active_users u
  JOIN jelly_favorite_colors ON jelly_favorite_colors.user_uuid = u.user_uuid
  JOIN jelly_colors ON jelly_colors.color_uuid= jelly_favorite_colors.color_uuid
  WHERE jelly_colors.name = 'green'