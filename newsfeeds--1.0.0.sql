-- ===========================================================================
-- newsfeeds PostgreSQL extension
-- Miles Elam <miles@geekspeak.org>
--
-- Depends on plpgsql
-- ---------------------------------------------------------------------------


-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION newsfeeds" to load this file. \quit

CREATE TABLE newsfeeds (
    id smallserial NOT NULL PRIMARY KEY,
    url character varying(256) NOT NULL UNIQUE,
    entries text NOT NULL,
    exclude_selector text,
    label_selector text,
    title_selector text NOT NULL,
    link_selector text NOT NULL,
    discussion_selector text,
    feedname character varying(256) NOT NULL,
    updated timestamp without time zone DEFAULT (now() - '7 days'::interval) NOT NULL,
    update_interval interval DEFAULT '00:30:00'::interval NOT NULL,
    added timestamp without time zone DEFAULT now() NOT NULL,
    last_id character varying(64)
);


COMMENT ON TABLE newsfeeds IS
'List of news aggregators and CSS selectors used to find relevant information about their links.';

CREATE TABLE headlines (
    id serial NOT NULL PRIMARY KEY,
    newsfeed smallint FOREIGN KEY (newsfeed)
                        REFERENCES newsfeeds(id) ON UPDATE CASCADE ON DELETE RESTRICT,
    source character varying(100),
    https boolean DEFAULT false NOT NULL,
    url text NOT NULL UNIQUE,
    metadata jsonb NOT NULL,
    discussion text,
    labels character varying(100)[],
    added timestamp without time zone DEFAULT now() NOT NULL,
    fts tsvector,
    archived timestamp without time zone,
    teaser_image text,
    content text,
    summary text,
    favicon text
);

COMMENT ON TABLE headlines IS
'Raw article information. Should be purged if deemed sufficiently old and not incorporated into an'
|| ' episode.';

COMMENT ON COLUMN headlines.source IS
'e.g., New York Times, Wired, BBC';

COMMENT ON COLUMN headlines.url IS
'URL without a protocol so that an https:// link is not considered distinct from http://.';

COMMENT ON COLUMN headlines.discussion IS
'Link to conversation about the topic if anyone needs some perspective.';

COMMENT ON COLUMN headlines.fts IS
'Full Text Search vector';

COMMENT ON COLUMN headlines.archived IS
'If a link goes dead, reference the last archive.org grab.';

COMMENT ON COLUMN headlines.metadata IS
'Raw headline information. Dumping ground for any extra info that doesn''t need direct reference'
|| ' from queries.';

COMMENT ON COLUMN headlines.https IS
'Whether a secure URL is available';

COMMENT ON COLUMN headlines.newsfeed IS
'The news feed (aggregator) this headline was crawled from. No feed implies a person manually'
|| ' added it.';

COMMENT ON COLUMN headlines.teaser_image IS
'Article-supplied thumbnail image';

COMMENT ON COLUMN headlines.content IS
'Page content; used for search.';

COMMENT ON COLUMN headlines.summary IS
'Not used currently. Placeholder for Chandler''s article summary algorithm.';

COMMENT ON COLUMN headlines.favicon IS
'Used for headline display to help differentiate sources easily.';

CREATE INDEX added_idx ON headlines USING btree (added DESC NULLS LAST);

CREATE INDEX fts_idx ON headlines USING gin (fts);

CREATE FUNCTION clean_query(query text) RETURNS text
LANGUAGE sql IMMUTABLE STRICT LEAKPROOF AS $$
  WITH raw_query AS
    (SELECT regexp_matches(replace(replace(replace(query, ':title', ':A'), ':description', ':B'), ':content', ':D'), '(\(?)\s*(''[^'']+''|"[^"]+"|[-''a-z0-9]+)\s*(\)?)(:\w)?\s*([&|]?)\s*', 'g') AS tokens)
  SELECT rtrim(string_agg(concat(tokens[1], tokens[2], tokens[3], coalesce(nullif(tokens[4], ''), '&')),''),'&')
    FROM raw_query;
$$;

COMMENT ON FUNCTION clean_query(query text) IS
'Convert the user search query into something the full text search query engine can handle.';

CREATE FUNCTION fts() RETURNS trigger
LANGUAGE plpgsql AS $$
  BEGIN
  NEW.fts :=
    setweight(to_tsvector(coalesce((new.metadata->'title')::text,'')), 'A') ||
    setweight(to_tsvector(coalesce((new.metadata->'description')::text, '')), 'B') ||
    setweight(to_tsvector(coalesce((new.source)::text, '')), 'C') ||
    setweight(to_tsvector(coalesce(new.content, '')), 'D');
  return NEW;
  END
$$;

COMMENT ON FUNCTION fts() IS
'Make sure the full text search (FTS) vector is updated for the search index whenever a change is'
|| ' made to the headline.';

CREATE FUNCTION hack_favicon() RETURNS void
LANGUAGE sql LEAKPROOF AS $$
  UPDATE aggregator.headlines
    SET favicon = array_to_string(regexp_matches(aggregator.reify_url(https, url)::text,
        '^https?://[^/]+/')::text[], ''::text) || 'favicon.ico'
    WHERE favicon IS NULL OR favicon = 'favicon.ico';
$$;

COMMENT ON FUNCTION hack_favicon() IS
'Convert relative favicon URLs to fully qualified URLs.';

CREATE FUNCTION headlines(since interval DEFAULT '7 days'::interval,
                          querytext text DEFAULT ''::text,
                          min_rank real DEFAULT 0.1,
                          lim integer DEFAULT 2000,
                          oset integer DEFAULT 0)
            RETURNS TABLE(id integer, rank real, added date, type character varying,
                          source character varying, title character varying, url text,
                          description text, discussion text, locale character varying,
                          teaser_image text, favicon text, tags character varying[])
LANGUAGE sql STABLE STRICT LEAKPROOF AS $$
  WITH ts AS (SELECT length(querytext) > 0 AS has_query,
                     gs.to_tsquery('english', querytext) AS query, now() - since AS cutoff)
  (SELECT id, ts_rank(fts, ts.query) * rank_modifier(added) as rank, added::date,
          metadata->>'type', source, coalesce(title, metadata->>'title'), reify_url(https, url),
          coalesce(description, metadata->>'description'), discussion, metadata->>'locale',
          teaser_image, favicon, labels
     FROM headlines h, ts
     WHERE ts.has_query = true AND added > cutoff AND fts @@ ts.query
           AND ts_rank(fts, ts.query) * aggregator.rank_modifier(added) >= min_rank
     ORDER BY rank DESC, id DESC
     LIMIT lim
     OFFSET oset)
  UNION ALL
  (SELECT id, 10.0 AS rank, added::date, metadata->>'type', source,
          coalesce(title, metadata->>'title'), reify_url(https, url),
          coalesce(description, metadata->>'description'), discussion, metadata->>'locale',
          teaser_image, favicon, labels
     FROM headlines h, ts
     WHERE ts.has_query = false AND added > cutoff
     ORDER BY id DESC
     LIMIT lim
     OFFSET oset)
  ORDER BY rank DESC, id DESC;
$$;

COMMENT ON FUNCTION headlines(since interval, querytext text, min_rank real, lim integer,
                              oset integer) IS
'Browse and search incoming news headlines.';


CREATE FUNCTION headlines_as_json(since interval DEFAULT '7 days'::interval,
                                  query text DEFAULT ''::text, min_rank real DEFAULT 1.0,
                                  lim integer DEFAULT 2000, oset integer DEFAULT 0) RETURNS jsonb
LANGUAGE sql STABLE STRICT LEAKPROOF AS $$
  SELECT coalesce(jsonb_agg(headlines), '[]'::jsonb)
    FROM headlines(since, query, min_rank, lim, oset) as headlines;
$$;


COMMENT ON FUNCTION headlines_as_json(since interval, query text, min_rank real, lim integer,
                                      oset integer) IS
'Browse and search incoming news headlines, returning them as JSON.';

CREATE FUNCTION pending_feeds() RETURNS json
LANGUAGE sql STABLE STRICT LEAKPROOF AS $$
  SELECT coalesce(array_to_json(array_agg(row_to_json(t))), '[]'::json)
    FROM (SELECT id, url || coalesce(last_id, '') AS url, entries, exclude_selector,
                 label_selector, title_selector, link_selector, discussion_selector, feedname,
                 updated, update_interval, added, last_id
            FROM newsfeeds
            WHERE updated + update_interval < now()
            ORDER BY id) AS t;
$$;

COMMENT ON FUNCTION pending_feeds() IS
'Used by gs-feeds to grab headlines by providing viable news aggregation feeds.';

CREATE FUNCTION source(source character varying, url character varying) RETURNS character varying
LANGUAGE sql IMMUTABLE STRICT LEAKPROOF AS $$
  SELECT coalesce(source, regexp_replace(url, '^https?://(?:www\.)?([^/]+)/.+$', '\1'));
$$;

COMMENT ON FUNCTION source(source character varying, url character varying) IS
'Provide a headline''s source either by explicit value or by using the domain name.';
