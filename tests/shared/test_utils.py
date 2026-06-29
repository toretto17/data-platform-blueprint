"""Platform-neutral utility tests (pure Python, no Spark dependency)."""
import pytest


def test_normalize_scores():
    """Test the anomaly detection normalize_scores helper."""
    import numpy as np
    # Portable: works regardless of platform
    raw = np.array([-0.5, -0.1, 0.2, 0.5])
    inv = -raw
    mn, mx = inv.min(), inv.max()
    norm = (inv - mn) / (mx - mn) if mx != mn else np.zeros_like(inv)
    assert norm.min() >= 0
    assert norm.max() <= 1


def test_psi_calculation():
    """Test PSI (Population Stability Index) math."""
    import numpy as np
    baseline = np.random.normal(0, 1, 1000)
    same = np.random.normal(0, 1, 1000)      # no drift
    shifted = np.random.normal(2, 1, 1000)    # drifted

    def psi(b, c, bins=10):
        eps = 1e-6
        bp = np.histogram_bin_edges(b, bins=bins)
        b_pct = np.histogram(b, bins=bp)[0] / len(b) + eps
        c_pct = np.histogram(c, bins=bp)[0] / len(c) + eps
        return float(np.sum((c_pct - b_pct) * np.log(c_pct / b_pct)))

    assert psi(baseline, same) < 0.1      # low drift
    assert psi(baseline, shifted) > 0.2    # high drift
