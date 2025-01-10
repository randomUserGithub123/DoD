
total_votes = 5
gamma = 0.5

quorum_threshold = (5 * total_votes - 2 * gamma * total_votes + 2 * gamma - 1) / 4
print(quorum_threshold)

