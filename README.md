# The Minority Client

> *Run the minority client!* ~Danny Ryan and/or Tim Beiko

As of writing, Ethereum has multiple client implementations, but [Geth / go-ethereum](https://github.com/ethereum/go-ethereum) stands out as a majority client with around 80-90% network ownership. Although this is an ode the client's stability and to its developers, it is also a situation with undesirable consequences.

In Ethereum 1 world, a single client ruling most of the network has well understood downsides:

- If Geth crashes via a DoS attack, users relying on it will be unable to transact or follow the chain.
- If Geth has a consensus bug, users relying on it will see a different version of the network state.

The former issue is somewhat bad as it causes an outage, but that's about the worst that can happen. The latter issue, however, is especially bad, as it can cause double spends by irreversibly reacting to a false (invalid) state of the network.  High profile users (e.g. exchanges) work around the above issues by running multiple flavor clients side by side and raise an alarm (e.g. disable deposits / withdrawals) when they disagree.

Mining pools usually also run multiple client flavors, though for them, it's worthwhile to mine on both sides of a chain split until the devs figure out what's happening, since that hedges them against losing all their earnings in case of being on the wrong side of history. Either way, the chain can still progress, the invalid side-chain being eventually orphaned. Life goes on.

In Ethereum 2 world, a new potential downside is dropped into the mix:

- If 1/3 + 1 validators of the network have a consensus bug, the network cannot finalize anymore.
- If 2/3 validators of the network have a consensus bug, the invalid chain gets finalized.

The former case is again the lesser problem, users waiting for chain finality will never receive it, so their operations might go "offline" until the issue is fixed. The latter case however is a big problem, as an invalid state is marked final and may be acted upon. It can be fixed with a rollback to the last good state (same as on the PoW chain), but it invalidates the notion of *finality*.

There have been some proposals to "enshrine" majority-client bugs into the protocol to avoid reorging finality, but that just pours oil onto the fire. Instead of incentivizing validators to run other client flavors, the devs would seemingly punish them for it as all blocks produced with valid-but-minority clients could get orphaned. This would essentially lock in a 100% single-client network.

The alternative proposal of asking people to run a minority client falls on deaf ears too (and has been for years) simply because why would anyone want to run a less stable client if a - most of the time - better is available? Maintaining infrastructure is time-consuming and people have better things to do than babysit something potentially unstable.

Seemingly we have a conflict here: running Geth seems easy and good for the user, but may potentially harm the network; not running Geth seems brittle and annoying for the user, but may potentially save the network. Since it's unfair to request validators to run a minority client (and bear the brunt of any issues), this project aims to make a different request: *Run minority clients **too**, that act as sentries for your favorite client*.

## Agreement orchestration

Before going into what the `minority` project is, it's important to emphasize what it isn't. Although our stated goal is to get users to run minority clients (too), this project is **not** about actually setting up and running Ethereum clients. There are various projects that permit home users to easily run one client or another (e.g. [DappNode](https://dappnode.io/)), but once we're reaching production infra requirements, it depends too much on individual use cases, budget constraints and devops capacity to come up with the "best" solution on what to run, how many, where and how to deploy.

The `minority` project instead assumes that validators are already familiar with how best to deploy to their own infrastructure; as well as how to provision and maintain different standalone clients in a reasonably stable way. The goal is to be a communication layer between consensus and execution clients, allowing anyone to run multiple clients (majority, minority, combination) and have an N-out-of-M agreement before accepting a state change (whether that's an execution result or a consensus update). 

E.g.

- The `minority` orchestrator can ensure that updating the chain head in execution clients only happens if 2/3 consensus clients agree on the new head (e.g. Lighthouse and Lodestar agree, Teku disagrees).
- The `minority` orchestrator can ensure that accepting an execution payload only happens if 2/3 execution clients agree on the new state root (e.g. Geth and Nethermind agree, OpenEthereum disagrees).

An added benefit of an advanced communication layer between the consensus/execution clients is capability for uniformly gathering and reporting behavioral metrics across the various client flavors; and potentially detecting operational degradations before they spiral out of control, causing an outage. A communication middleware also permits uniformly gathering an audit trail of events passing between the two layers, potentially aiding debugging client issues. 

## F.A.Q.

**Running an execution client is expensive! Isn't it overkill to ask validators to run 2-3?**

At the time of writing, 1 ETH = $3785. Running a single validator requires an initial deposit of 32 ETH, or $120K in fiat terms. At that funding magnitude, we feel the cost of running 3 execution clients side by side to support the validator is an acceptable security investment.

**Running an extra middleware is more work! Why don't consensus clients talk to multiple execution clients directly?**

Decoupling the multiplexing between consensus and execution clients permits them to be swapped out at any point in time without unexpected behavioral changes. Having the multiplexer reimplemented in either side would entail at minimum slight differences that may end up requiring topology reconfigurations to change an underlying component.

**Running a distributed multiplexer is non-obvious. Wouldn't a central orchestrator be simpler?**

A central server is undoubtedly simpler, but it is also a single point of failure, whether that's hardware failure, software errors or machine overload. We have no control over the load that consensus/execution clients generate, so keeping them somewhat isolated seems safer in the face of bugs. A decentralized architecture might also prove easier to scale horizontally.

**Running the multiplexer beside each client is weird? Why not use an orchestration cluster?**

Running an extra process beside each client is indeed more work than simply pointing them to an orchestration cluster, but it keeps the complexity down as consensus/execution clients still live in a 1-to-1 world. Surfacing the *cluster* concept into either client layer would require those clients to meaningfully handle 1-to-N connections, which is what we're trying to avoid in the first place.  