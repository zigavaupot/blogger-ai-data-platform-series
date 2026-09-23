![Asking the live data: Claude connected to Oracle Analytics Cloud through the OAC MCP server, querying the AIDP gold layer fed by live TfL streams](https://zigavaupot.github.io/blogger-ai-data-platform-series/oac-mcp-server-and-aidp-streams/images/oac-mcp-server-and-aidp-streams.png)

# Asking the Live Data: Claude, the OAC MCP Server and AIDP Streams (Part 8 of the AI Data Platform Series)

*This is the eighth post in the AI Data Platform series. Over the last two posts, the gold layer grew three live pieces: an arrivals board for every bus stop in London, a stations reference table, and estimated positions for every bus on line 25, all queryable from Oracle Analytics Cloud. Until now, every question I asked that data was either SQL in a notebook or a workbook in OAC. This time I connected Claude to OAC through Oracle Analytics' MCP server and simply asked, in plain language, what was going on.*

In this post I'll walk through:

- **The chain, end to end**: from a live TfL prediction to an answer in a chat window, and where the MCP server sits in it.
- **Connecting Claude to OAC**: the MCP Connect page in OAC, and the small local bridge it gives you.
- **A conversation with live data**: seven questions, from "what data is available" to planning a trip across the City, with the logical SQL behind the answers.
- **What the conversation exposed**: things about my own datasets I hadn't noticed, and one I had noticed but hadn't fixed.

## The chain

It's worth laying out how many pieces are between a bus in London and an answer in Claude:

1. TfL's Unified API publishes arrival predictions.
2. The stream producer pushes them into **OCI Streaming** ([stage 1](https://zigavaupot.blogspot.com/2026/09/ai-data-platform-series-oci-streaming.html)).
3. **Bronze** lands every message as-is ([stage 2](https://zigavaupot.blogspot.com/2026/09/ai-data-platform-series-bronze-layer.html)).
4. **Silver** keeps the latest prediction per bus, stop, line and direction ([stage 3](https://zigavaupot.blogspot.com/2026/09/ai-data-platform-series-silver-layer.html)).
5. **Gold** turns that into `arrivals_board` (a live view), `stations`, and `vehicle_positions` (recomputed every 30 seconds) ([stage 4a](https://zigavaupot.blogspot.com/2026/09/ai-data-platform-series-gold-layer-arrivals-board.html), [stage 4b](https://zigavaupot.blogspot.com/2026/09/ai-data-platform-series-gold-layer-stations.html)).
6. **OAC** reads gold through the Oracle AI Data Platform connection, as three datasets.
7. The **OAC MCP server** exposes those datasets as tools an AI assistant can call.
8. **Claude** decides which tool to call, writes the query, and explains the result.

The important part is step 7. Claude never connects to AIDP directly. It goes through OAC, as me, with my OAC permissions, and it sees the data the way OAC's datasets define it: friendly column names, measures, aggregation rules. That turns out to matter a lot, as you'll see.

## Connecting Claude to OAC

In OAC, the MCP settings live in your user profile: click your avatar, open **Profile**, and select **MCP Connect**.

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/oac-mcp-server-and-aidp-streams/images/mcp-oac-profile-mcp-connect.png" alt="OAC home page with the user profile dialog open on the MCP Connect tab, showing Client ID and Scope for MCP OAuth and the MCP Connect Tool section; instance-specific values redacted">
  <figcaption>OAC Profile, MCP Connect tab. Instance-specific values redacted.</figcaption>
</figure>

The page offers two ways in:

- **Client ID and scope for MCP OAuth**: for MCP clients that connect to a remote MCP server themselves and handle the OAuth flow.
- **MCP Connect Tool**: a small Node.js bridge you download and run locally. Your MCP host (Claude's desktop app, in my case) starts it, and it handles the connection to your OAC instance. The first time, it opens a browser window where you log in with your normal OAC credentials.

I used the MCP Connect Tool. The steps are the ones on the page: install Node.js 18 or later, unzip the download, and add the configuration it shows to your MCP host's configuration:

```json
{
  "mcpServers": {
    "oac-mcp-connect": {
      "command": "node",
      "args": [
        "/path/to/oac-mcp-connect.js",
        "https://<your-oac-instance>.analytics.ocp.oraclecloud.com"
      ]
    }
  }
}
```

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/oac-mcp-server-and-aidp-streams/images/mcp-oac-mcp-connect-detail.png" alt="Close-up of the MCP Connect Tool section: Download button, installation steps (Node.js 18, unpack the zip, copy the MCP server configuration) and the configuration JSON with the instance URL redacted">
  <figcaption>The MCP Connect Tool: download, unzip, paste the configuration into your MCP host.</figcaption>
</figure>

After restarting Claude and logging in once in the browser, `oac-mcp-connect` shows up as a connected tool source. The server provides a set of tools, among them tools to discover and describe data sources (`discover_data`, `describe_data`), search the catalog, and run queries: `execute_logical_sql`, which runs OAC's own logical SQL against a dataset or subject area. Claude decides which ones to call.

I ran everything below in Claude's desktop app, in Cowork mode.

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/oac-mcp-server-and-aidp-streams/images/mcp-cowork-first-prompt.png" alt="Claude desktop app, Cowork mode, with the first prompt typed: what data is available">
  <figcaption>The first question. No mention of OAC, TfL or buses.</figcaption>
</figure>

## A conversation with live data

### 1. "What data is available?"

I deliberately started without any context: no mention of OAC, datasets, or buses. Claude went to the OAC tools on its own and listed what my instance has: one subject area and nine datasets, with folders and owners. Among them, the three from this series, in `shared/AIDP demo`: `tfl - arrivals board`, `tfl - stations`, and `tfl - vehicle positions`.

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/oac-mcp-server-and-aidp-streams/images/mcp-claude-available-data.png" alt="Claude's answer listing 1 subject area and 9 datasets with folder, owner and notes, including the three TfL datasets in shared/AIDP demo">
  <figcaption>Everything my OAC user can see: one subject area, nine datasets.</figcaption>
</figure>

It also noticed details I'd forgotten about: two different datasets both called "Returns2", and that `tfl - vehicle positions` has the internal name "Bus Positions". Nothing unusual, just an honest inventory of a demo instance.

### 2. "Describe datasets that start with tfl"

This one turned into a small review of my own work.

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/oac-mcp-server-and-aidp-streams/images/mcp-claude-describe-tfl.png" alt="Claude describing the three TfL datasets: columns, how they connect on Naptan ID and Line ID, and a 'worth fixing before the demo' list: wrong default aggregation, a column name typo, an inconsistent dataset name, and no column descriptions">
  <figcaption>A description of the three datasets, and a list of things worth fixing.</figcaption>
</figure>

The description itself is accurate: columns grouped sensibly, and how the three datasets relate (arrivals and stations on Naptan ID, positions to stations twice, through the from and to stops). The more useful part was the list at the end, all of it correct:

- **Wrong default aggregation.** When OAC created the datasets, it made every numeric column a measure that aggregates with `SUM`. For *Seconds to Arrival*, *Minutes to Arrival* and *Arrival Rank*, a sum is meaningless. `progress` should be an average, if anything.
- **A typo**: `Logitude` in the stations dataset. Mine.
- **Inconsistent names**: "tfl - vehicle positions" on the outside, "Bus Positions" inside, which is what queries have to use.
- **No column descriptions.** That one matters more than it sounds. Column descriptions are exactly what an assistant like this (or OAC's own AI features) uses to understand the data.

None of this is visible when *I* use the datasets, because I know what the columns mean. It becomes visible the moment something else has to understand them from the metadata alone.

### 3. "Which buses are arriving to Aldgate Station?"

Now a real question.

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/oac-mcp-server-and-aidp-streams/images/mcp-claude-aldgate-arrivals.png" alt="Claude's answer: three buses coming to Aldgate Station (25 to Ilford in 4 min, 254 to Holloway in 4 min, 242 to Aldgate in 14 min) and three more at nearby Aldgate East Station">
  <figcaption>Aldgate: the next buses, from the live arrivals board.</figcaption>
</figure>

Behind the answer is one `execute_logical_sql` call. This is the request Claude sent, with the dataset reference shortened for readability (in the real query it's the dataset's ID and name, `XSA('<id>'.'tfl - arrivals board')`):

```sql
SELECT
  XSA(...)."arrivals_board"."Station Name",
  XSA(...)."arrivals_board"."Naptan ID",
  XSA(...)."arrivals_board"."Platform",
  XSA(...)."arrivals_board"."Line Name",
  XSA(...)."arrivals_board"."Destination",
  XSA(...)."arrivals_board"."Vehicle ID",
  XSA(...)."arrivals_board"."Expected Arrival",
  XSA(...)."arrivals_board"."Arrival Status",
  XSA(...)."arrivals_board"."Board as of",
  MIN(OVERRIDEAGGR(XSA(...)."arrivals_board"."Minutes to Arrival"))
FROM XSA(...)
WHERE UPPER(XSA(...)."arrivals_board"."Station Name") LIKE '%ALDGATE%'
ORDER BY 7
FETCH FIRST 200 ROWS ONLY
```

A few things I like about this query:

- `XSA(...)` is how logical SQL addresses a dataset directly, rather than a subject area. Claude picked that up from the tool descriptions.
- `MIN(OVERRIDEAGGR(...))` forces a `MIN` instead of the dataset's default `SUM` on *Minutes to Arrival*. That's the aggregation problem from the previous question, worked around in the query.
- `LIKE '%ALDGATE%'` is a broad match, which is why the answer includes Aldgate East as well, clearly separated as "nearby".

The response came back with 6 rows, and OAC reported a streaming duration of 5 ms. The board was "as of 12:14:12", which is the view's `board_as_of`: the moment the query ran.

### 4. "And now?"

Two words, and the most satisfying answer in the whole conversation.

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/oac-mcp-server-and-aidp-streams/images/mcp-claude-aldgate-and-now.png" alt="Claude's answer to 'and now?': the board refreshed as of 12:17:35, the 25 and 254 now due in 1 min, the 242 in 10 min, and a second 25 to Holborn added at 16 min">
  <figcaption>Three minutes later: the countdowns moved, and a new bus appeared.</figcaption>
</figure>

Claude re-ran the query. The board was now as of 12:17:35. The 25 and the 254 that were 4 minutes away were now 1 minute away, and a second 25, towards Holborn, had appeared on the board. Claude pointed that out itself: "added to the board since the last check".

This is the whole chain from the list above working at once. Nothing was refreshed by hand. The `arrivals_board` view simply re-evaluated against the latest silver data, which the streams keep up to date, and OAC and the MCP server passed the new result through.

### 5. "How many buses are currently on line 25, by direction?"

A switch to a different dataset, and an aggregate instead of a list.

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/oac-mcp-server-and-aidp-streams/images/mcp-claude-line25-count.png" alt="Claude's answer: 20 buses on line 25 in the 12:18:36 snapshot, 11 inbound and 9 outbound, with a note that it hasn't verified which direction is which">
  <figcaption>20 buses on line 25: 11 inbound, 9 outbound.</figcaption>
</figure>

The query was a `COUNT(DISTINCT "Vehicle ID")` grouped by direction, with `MAX("Computed at")` to report the snapshot time: 12:18:36, which is the continuous job from the last post doing its work every 30 seconds.

I also liked that the answer flagged its own uncertainty: it *guessed* that inbound means towards Holborn, and said it hadn't checked that against the data.

### 6. "Display buses on line 25 on the map"

I expected Claude to point me to the OAC workbook. Instead, it pulled the positions and the line 25 stops and drew a map itself.

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/oac-mcp-server-and-aidp-streams/images/mcp-claude-line25-map.png" alt="A map drawn by Claude: line 25 stops as small grey points from Holborn Circus through Aldgate, Mile End, Stratford and Green Street to Ilford Broadway, with 12 inbound buses (blue) and 11 outbound buses (orange), followed by notes on direction, bunching and progress values">
  <figcaption>Claude's own map of line 25, from the vehicle_positions and stations datasets.</figcaption>
</figure>

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/oac-mcp-server-and-aidp-streams/images/mcp-claude-line25-map-full.jpg" alt="The same line 25 bus positions on a full street map of east London, inbound buses in blue and outbound in orange along the route from Tower Hamlets through Stratford to Ilford">
  <figcaption>The same snapshot on a street map.</figcaption>
</figure>

The notes under the map are the interesting part:

- **It corrected itself.** Checking against the arrivals board, where SK20AYP was inbound to Ilford and SK20AYY outbound to Holborn, it found it had the directions backwards in the previous answer, and said so.
- **Buses bunch at both ends**: three near Hainault Street in Ilford, four around St Paul's. Layovers at the terminus, or real bunching.
- **19 of 23 buses have `progress` exactly 1.0**, so they're drawn at a stop, not between stops.

That last point is the same limitation I described in the previous post. `progress` is capped at 1 when a bus has passed its predicted next stop but TfL hasn't sent a new prediction yet. I knew about it; Claude found it on its own, from the data, in about a minute.

### 7. A trip, and a timezone

For the last question I asked something practical: how long would it take me to get from Tower Bridge to St Paul's on the 25?

<figure>
  <img src="https://zigavaupot.github.io/blogger-ai-data-platform-series/oac-mcp-server-and-aidp-streams/images/mcp-claude-trip-utc.png" alt="Claude's trip plan from Tower Bridge to St Paul's via line 25 from Aldgate, with alternatives, followed by the question 'aren't we looking at bus schedules that already passed?' and Claude's answer that the data is in UTC while London is on BST">
  <figcaption>A trip plan built from live arrivals and positions, and the timezone question.</figcaption>
</figure>

The answer combined both datasets: walk to Aldgate stop R, the bus arriving now will be gone by then, the next one (SK20AYH) was at Stepney Green and should reach Aldgate in a few minutes, though "it isn't on the Aldgate board yet", so that part is an estimate. It also said, honestly, that the 25 isn't the fastest way, and suggested the Tube or walking.

Then I noticed the times. At 13:35 in London, the plan said I'd arrive at 12:58. So I asked: "aren't we looking at bus schedules that already passed?"

No. The data is current, but every timestamp in the pipeline is **UTC**, and London is on British Summer Time, one hour ahead. The bronze, silver and gold layers all store TfL's timestamps as they arrive, in UTC; OAC shows them as-is. I'd been reading UTC times in notebooks for weeks without it mattering. Asking a question in plain language, about *my* afternoon, is exactly when it does matter.

Claude's suggestion was the right one: convert to London time in the dataset (or a data flow), so nobody reads a live board as stale.

## What the conversation exposed

I expected this post to be about the MCP server. It turned out to be just as much about the data behind it.

The MCP part was the easy part. Download a bridge, paste some JSON, log in once. From there, Claude worked out which datasets to use, wrote reasonable logical SQL, and re-ran it when asked "and now?". Nothing had to be built specially for it. The same OAC datasets that feed the workbooks feed the conversation, with the same permissions.

What mattered was everything a human user fills in from their own knowledge, and an assistant can't:

- **Aggregation defaults.** `SUM` on a countdown is harmless in a table visualization where I never sum it. For an assistant, it's a trap, and a careful one works around it with `OVERRIDEAGGR`.
- **Names and descriptions.** A typo, an internal name that doesn't match the display name, no column descriptions. Each one costs a little accuracy.
- **Meaning that isn't in the data.** Which direction is "inbound" on line 25? The data doesn't say. Claude guessed, then checked, then corrected itself.
- **Timezones.** UTC is the right way to store timestamps. It's not the right way to show them to someone standing at a bus stop in London.
- **Known limitations don't stay hidden.** The `progress = 1.0` issue came up by itself, from the data alone.

Before I show this in a demo, I'll fix the list: proper aggregation rules (or attributes instead of measures), the typo, consistent names, column descriptions, and London time in the datasets.

## What's next

That's the pipeline running end to end: live TfL data through OCI Streaming, AIDP's bronze, silver and gold layers, Oracle Analytics Cloud, and now a conversation on top of it all.

Next, I'll clean up the datasets based on what this conversation found, then come back to the open questions from the gold layer: positions for all bus lines, not just line 25, and fixing the `progress` cap so buses don't wait at stops on the map.

*Related: [Just Streams: Real-Time Data Pipelines on OCI](https://zigavaupot.blogspot.com/2026/08/ai-data-platform-series-just-streams.html) (series intro), [Gold Layer: The Arrivals Board and Oracle Analytics Cloud](https://zigavaupot.blogspot.com/2026/09/ai-data-platform-series-gold-layer-arrivals-board.html), [Gold Layer: Stations and Live Bus Positions](https://zigavaupot.blogspot.com/2026/09/ai-data-platform-series-gold-layer-stations.html)*
