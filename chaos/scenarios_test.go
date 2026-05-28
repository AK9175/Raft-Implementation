package chaos

import "testing"

func TestScenario_SplitBrain(t *testing.T)               { scenarioSplitBrain(t) }
func TestScenario_StaleLogCannotWin(t *testing.T)        { scenarioStaleLogCannotWin(t) }
func TestScenario_LeaderIsolationWriteLoss(t *testing.T) { scenarioLeaderIsolationWriteLoss(t) }
func TestScenario_PacketLossStillConverges(t *testing.T) { scenarioPacketLossStillConverges(t) }
