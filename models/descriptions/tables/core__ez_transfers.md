{% docs core__ez_transfers %}

This table contains a flattened easy version of the native transfers. This table uses the fact_transfers table as a base and filters down to only Movement (Aptos) tokens. The logic used to derive this table requires the withdrawal event to occur in the previous event to the deposit event and also requires the withdrawal event to be the same amount as the deposit event. The only exception to that rule is when a "CoinRegisterEvent" occurs in between the withdraw and deposit events. Any transfers that do not meet this criteria are not included in this table.

{% enddocs %}
