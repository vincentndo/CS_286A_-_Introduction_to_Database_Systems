-- Q5 Iteration

DROP TABLE IF EXISTS q5_extended_paths;
CREATE TABLE q5_extended_paths(src, dest, length, path)
AS
    SELECT DISTINCT e.src, u.dest, u.length + 1, array_prepend(e.src, u.path) AS path  -- remember to remove duplicates!
    FROM q5_paths_to_update u, q5_edges e
    WHERE e.dest = u.src AND e.src != u.dest
    UNION
    SELECT DISTINCT u.src, e.dest, u.length + 1, array_append(u.path, e.dest) AS path
    FROM q5_paths_to_update u, q5_edges e
    WHERE u.dest = e.src AND u.src != e.dest
;

CREATE TABLE q5_new_paths(src, dest, length, path)
AS
   SELECT e.src, e.dest, e.length, e.path
   FROM q5_extended_paths e
   WHERE NOT EXISTS (SELECT * FROM q5_paths p WHERE p.src = e.src AND p.dest = e.dest)
   UNION
   SELECT e.src, e.dest, e.length, e.path
   FROM q5_extended_paths e, q5_paths p WHERE p.src = e.src AND p.dest = e.dest AND e.length < p.length
;

CREATE TABLE q5_better_paths(src, dest, length, path)
AS 
    SELECT n.src, n.dest, CASE WHEN n.length < p.length THEN n.length ELSE p.length END length, CASE WHEN n.length < p.length THEN n.path ELSE p.path END path
    FROM q5_new_paths n, q5_paths p
    WHERE n.src = p.src AND n.dest = p.dest
    UNION
    SELECT p.src, p.dest, p.length, p.path
    FROM q5_paths p
    WHERE NOT EXISTS (SELECT * FROM q5_new_paths n WHERE n.src = p.src AND n.dest = p.dest)
    UNION
    SELECT n.src, n.dest, n.length, n.path
    FROM q5_new_paths n
    WHERE NOT EXISTS (SELECT * FROM q5_paths p WHERE n.src = p.src AND n.dest = p.dest)
;

DROP TABLE q5_paths;
ALTER TABLE q5_better_paths RENAME TO q5_paths;

DROP TABLE q5_paths_to_update;
ALTER TABLE q5_new_paths RENAME TO q5_paths_to_update;

SELECT COUNT(*) AS path_count,
       CASE WHEN 0 = (SELECT COUNT(*) FROM q5_paths_to_update) 
            THEN 'FINISHED'
            ELSE 'RUN AGAIN' END AS status
  FROM q5_paths;
