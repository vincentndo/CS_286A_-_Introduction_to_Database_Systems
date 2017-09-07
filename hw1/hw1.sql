DROP VIEW IF EXISTS q0, q1i, q1ii, q1iii, q1iv, q2i, q2ii, q2iii, q3i, q3ii, q3iii, q4i, q4ii, q4iii, q4iv;

-- Question 0
CREATE VIEW q0(era) 
AS
  SELECT MAX(era)
  FROM pitching
;

-- Question 1i
CREATE VIEW q1i(namefirst, namelast, birthyear)
AS
  SELECT namefirst, namelast, birthyear
  FROM master
  WHERE weight > 300
;

-- Question 1ii
CREATE VIEW q1ii(namefirst, namelast, birthyear)
AS
  SELECT namefirst, namelast, birthyear
  FROM master
  WHERE namefirst ~ '.* .*'
  ORDER BY namefirst
;

-- Question 1iii
CREATE VIEW q1iii(birthyear, avgheight, count)
AS
  SELECT birthyear, avg(height), count(*)
  FROM master
  GROUP BY birthyear
  ORDER BY birthyear
;

-- Question 1iv
CREATE VIEW q1iv(birthyear, avgheight, count)
AS
  SELECT birthyear, avgheight, count
  FROM q1iii
  WHERE avgheight > 70
  ORDER BY birthyear
;

-- Question 2i
CREATE VIEW q2i(namefirst, namelast, playerid, yearid)
AS
  SELECT m.namefirst, m.namelast, h.playerid, h.yearid
  FROM master m, halloffame h
  WHERE m.playerid = h.playerid AND h.inducted = 'Y'
  ORDER BY h.yearid DESC
;

-- Question 2ii
CREATE VIEW q2ii(namefirst, namelast, playerid, schoolid, yearid)
AS
  SELECT q.namefirst, q.namelast, q.playerid, c.schoolid, q.yearid
  FROM q2i q, collegeplaying c, schools s
  WHERE q.playerid = c.playerid AND c.schoolid = s.schoolid AND s.schoolstate = 'CA'
  ORDER BY q.yearid DESC, c.schoolid, q.playerid
;

-- Question 2iii
CREATE VIEW q2iii(playerid, namefirst, namelast, schoolid)
AS

--  // Another approach. If use UNION -> distinct rows
--  SELECT q.playerid, q.namefirst, q.namelast, c.schoolid
--  FROM q2i q, collegeplaying c
--  WHERE q.playerid = c.playerid
--  UNION ALL
--  SELECT q.playerid, q.namefirst, q.namelast, NULL
--  FROM q2i q
--  WHERE q.playerid NOT IN
--    (SELECT playerid
--    FROM collegeplaying)
--  ORDER BY playerid DESC, schoolid

-- // Duplicate answer
  SELECT q.playerid, q.namefirst, q.namelast, c.schoolid
  FROM q2i q LEFT OUTER JOIN collegeplaying c
  ON q.playerid = c.playerid
  ORDER BY q.playerid DESC, c.schoolid
;

-- Question 3i
CREATE VIEW q3i(playerid, namefirst, namelast, yearid, slg)
AS
  SELECT m.playerid, m.namefirst, m.namelast, b.yearid,
    ((b.h - b.h2b - b.h3b - b.hr) + 2 * b.h2b + 3 * b.h3b + 4 * b.hr)::FLOAT / b.ab AS slg
  FROM master m, batting b
  WHERE m.playerid = b.playerid AND b.ab > 50
  ORDER BY slg DESC, yearid, playerid
  LIMIT 10
;

-- Question 3ii
CREATE VIEW q3ii(playerid, namefirst, namelast, lslg)
AS
  SELECT m.playerid, m.namefirst, m.namelast,
    (SUM(b.h - b.h2b - b.h3b - b.hr) + 2 * SUM(b.h2b) + 3 * SUM(b.h3b) + 4 * SUM(b.hr))::FLOAT / SUM(b.ab) AS lslg
  FROM master m, batting b
  WHERE m.playerid = b.playerid
  GROUP BY m.playerid
  HAVING SUM(b.ab) > 50
  ORDER BY lslg DESC, playerid
  LIMIT 10
;

-- Question 3iii
CREATE VIEW q3iii(namefirst, namelast, lslg)
AS
  SELECT m.namefirst, m.namelast,
    (SUM(b.h - b.h2b - b.h3b - b.hr) + 2 * SUM(b.h2b) + 3 * SUM(b.h3b) + 4 * SUM(b.hr))::FLOAT / SUM(b.ab) AS lslg
  FROM master m, batting b
  WHERE m.playerid = b.playerid
  GROUP BY m.playerid
  HAVING SUM(b.ab) > 50 AND
    (SUM(b.h - b.h2b - b.h3b - b.hr) + 2 * SUM(b.h2b) + 3 * SUM(b.h3b) + 4 * SUM(b.hr))::FLOAT / SUM(b.ab) > ANY
      (SELECT (SUM(b.h - b.h2b - b.h3b - b.hr) + 2 * SUM(b.h2b) + 3 * SUM(b.h3b) + 4 * SUM(b.hr))::FLOAT / SUM(b.ab)
      FROM master m, batting b
      WHERE m.playerid = 'mayswi01' AND m.playerid = b.playerid
      GROUP BY m.playerid)
  ORDER BY namefirst
;

-- Question 4i
CREATE VIEW q4i(yearid, min, max, avg, stddev)
AS
  SELECT yearid, MIN(salary), MAX(salary), AVG(salary), STDDEV(salary)
  FROM salaries
  GROUP BY yearid
  ORDER BY yearid
;

CREATE VIEW hist_info(bin, max, min)
AS
  SELECT (MAX(salary) - MIN(salary)) / 10 AS bin, MAX(salary) AS max, MIN(salary) as min
  FROM salaries
  WHERE yearid = '2016'
  GROUP BY yearid
;

CREATE VIEW hist_0_to_8(binid, count)
AS
  SELECT FLOOR((salary - (SELECT min FROM hist_info)) / (SELECT bin FROM hist_info)) AS binid, count(*) AS count
  FROM salaries
  WHERE yearid = '2016' AND FLOOR((salary - (SELECT min FROM hist_info)) / (SELECT bin FROM hist_info)) < 9
  GROUP BY 1
  ORDER BY 1
;

CREATE VIEW hist_9_to_10(binid, count)
AS
  SELECT FLOOR((salary - (SELECT min FROM hist_info)) / (SELECT bin FROM hist_info)) AS binid, count(*) AS count
  FROM salaries
  WHERE yearid = '2016' AND FLOOR((salary - (SELECT min FROM hist_info)) / (SELECT bin FROM hist_info)) >=9
  GROUP BY 1
  ORDER BY 1
;

CREATE VIEW hist_simple(binid, count)
AS
  SELECT * FROM hist_0_to_8
  UNION ALL
  SELECT MIN(binid), SUM(count)
  FROM hist_9_to_10
  ORDER BY binid
;

-- Question 4ii
CREATE VIEW q4ii(binid, low, high, count)
AS
  SELECT binid, (SELECT min FROM hist_info) + binid * (SELECT bin FROM hist_info) AS low,
    (SELECT min FROM hist_info) + (binid + 1) * (SELECT bin FROM hist_info) AS high, count
  FROM hist_simple
  ORDER BY binid
;

-- Question 4iii
CREATE VIEW q4iii(yearid, mindiff, maxdiff, avgdiff)
AS
  SELECT q2.yearid, q2.min - q1.min, q2.max - q1.max, q2.avg - q1.avg
  FROM q4i q1, q4i q2
  WHERE q2.yearid - q1.yearid = 1
;

-- Question 4iv
CREATE VIEW q4iv(playerid, namefirst, namelast, salary, yearid)
AS
  SELECT m.playerid, m.namefirst, m.namelast, s.salary, s.yearid
  FROM master m, salaries s
  WHERE m.playerid = s.playerid AND
    (s.yearid = '2000' AND s.salary >= ALL
      (SELECT salary
      FROM salaries
      WHERE yearid = '2000')
    )
  UNION ALL
  SELECT m.playerid, m.namefirst, m.namelast, s.salary, s.yearid
  FROM master m, salaries s
  WHERE m.playerid = s.playerid AND
    (s.yearid = '2001' AND s.salary >= ALL
      (SELECT salary
      FROM salaries
      WHERE yearid = '2001')
    )
;

