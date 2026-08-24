package gateway

import "strings"

// bannedWords is a deliberately tiny, fake wordlist. Its only job is to
// prove the produce -> filter -> broadcast/reject pipeline shape works
// end to end — see IMPLEMENTATION_PLAN.md's "Chat filter for this
// milestone" decision. This is not real moderation: no rate limiting,
// no reporting, no bans. Real moderation is an explicitly deferred
// milestone, mitigated for now by rooms being private and code-joined.
var bannedWords = []string{"badword", "uglyword", "meanword"}

// isChatApproved reports whether text passes the stub filter.
func isChatApproved(text string) bool {
	lower := strings.ToLower(text)
	for _, w := range bannedWords {
		if strings.Contains(lower, w) {
			return false
		}
	}
	return true
}
