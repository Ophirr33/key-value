# key-value

Our program, which represents a single 'server', uses an object-oriented approach
to manage the replica's state. The replica, or shard as it is called in the running
while loop of our code, can be one of Follower, Candidate, or Leader. All three
classes focus around handle_msg and handle_timeout. handle_msg is responsible for
delegating to the appropriate handler method based on the message type.
handle_timeout is based on the replica's timeout field. For a leader, this timeout
represents how often the leader needs to send a heartbeat. It is also used to
determine whether the leader has lost a quorum (seen in partitioned networks, also
useful in extremely lossy networks). For a candidate, this timeout represents
how often the candidate should ask for votes, and when the candidate should continue
to the next term and start a new election. It is also assigned as a small random
number between elections to avoid split-vote scenarios. For a follower, the timeout
represents how long a follower will wait before becoming a candidate if it hasn't
heard from the leader.

In our implementation, the leader sends out a chunk of the log at a time to each
follower (max. length is 25 entries of the log). Followers will then send back
what index they are ok to commit to, and the leader will add a follower to all
appropriate quorums, kept as sets within the leader.

Elections are fairly simple. A candidate sends out four requests for votes within
one election period. This was done to deal with extremely lossy networks, as
candidates were having difficulty being elected within one term if only one
vote request was sent. Others will respond with a vote if they are in support of
the candidate. They will not send a message back if they are not voting for the
candidate.

In this project, we faced a number of challenges. Our first main challenge was
dealing with leader failure. The issue turned out to be a malformatted else case.
Quite embarrassing and a real sore point between us project partners. We are now
in full agreement that whitespace should not be syntax-critical. Our next main
challenge was dealing with messages received by a leader without a quorum (i.e.
in a network partition). Our initial solution involved said leader chopping off
the end of its log when it turned into a candidate or follower, so those messages
could be redirected to the proper leader, instead of being overwritten in the old
leader's log and left effectively unanswered. This solution proved to be completely
wrong once we reached the advanced tests, as we started receiving 'unexpected MID'
errors because we had sent a redirect message for those IDs and then occaisionally
committed them before the client had sent the redirected message because some other
replica had them in its log. We eventually solved this problem by having the leader
fall into candidate state if it failed to hear from a quorum within a second. We
also faced some small issues with latency and the drop rate in advanced-3, but those
issues were solved with some timeout tweaks and adding extra vote requests in
candidates.

Our code was tested thoroughly against the provided tests, and similar test scenarios
that we created to try and make some simulator errors that we were receiving rarely
appear more often. As usual, debug print statements were used heavily, especially to
diagnose inconsistencies in our system.

- Ceridwen Driskill, Ty Coghlan
