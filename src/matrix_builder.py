import numpy as np
import pandas as pd
import logging

logger = logging.getLogger("matrix_builder")

def build_exchange_matrix(rate_dict, currencies, base_currency="USD"):
    """
    Builds an NxN log exchange rate matrix from a dict of mid-prices.
    
    Parameters:
    -----------
    rate_dict : dict
        Dictionary of exchange rates, e.g. { "USD_EUR": 0.90, "USD_JPY": 110, ... }
    currencies : list
        List of currency codes, e.g. ["USD","EUR","JPY",...]
    base_currency : str
        The currency used as numeraire (default: "USD")
    
    Returns:
    --------
    pandas.DataFrame
        An NxN matrix with log exchange rates
    """
    # Validate inputs
    if not rate_dict:
        logger.error("Empty rate dictionary provided")
        raise ValueError("Rate dictionary cannot be empty")
        
    if base_currency not in currencies:
        logger.error(f"Base currency {base_currency} not in currency list")
        raise ValueError(f"Base currency {base_currency} must be in the currency list")
    
    # Check for missing pairs and log warnings
    for c in currencies:
        if c == base_currency:
            continue
            
        key_fwd = f"{base_currency}_{c}"
        key_rev = f"{c}_{base_currency}"
        
        if key_fwd not in rate_dict and key_rev not in rate_dict:
            logger.warning(f"No rate found for {base_currency}/{c} pair")

    # Build the matrix
    N = len(currencies)
    M = pd.DataFrame(index=currencies, columns=currencies, dtype=float)

    # Initialize diagonal
    for c in currencies:
        M.loc[c, c] = 0.0

    # Fill in direct rates for base currency
    missing_rates = 0
    for c in currencies:
        if c == base_currency:
            continue
            
        key_fwd = f"{base_currency}_{c}"  # e.g. "USD_EUR"
        key_rev = f"{c}_{base_currency}"  # e.g. "EUR_USD"

        if key_fwd in rate_dict:
            # base -> c
            rate = rate_dict[key_fwd]
            if rate <= 0:
                logger.error(f"Invalid rate {rate} for {key_fwd}")
                missing_rates += 1
                M.loc[base_currency, c] = None
                M.loc[c, base_currency] = None
            else:
                M.loc[base_currency, c] = np.log(rate)
                M.loc[c, base_currency] = -np.log(rate)
                
        elif key_rev in rate_dict:
            # c -> base
            rate = rate_dict[key_rev]
            if rate <= 0:
                logger.error(f"Invalid rate {rate} for {key_rev}")
                missing_rates += 1
                M.loc[base_currency, c] = None
                M.loc[c, base_currency] = None
            else:
                # We can invert it
                val = 1.0 / rate
                M.loc[base_currency, c] = np.log(val)
                M.loc[c, base_currency] = -np.log(val)
        else:
            # If neither pair is in rate_dict, you can skip or handle error
            missing_rates += 1
            M.loc[base_currency, c] = None
            M.loc[c, base_currency] = None
            logger.warning(f"Missing rate for pair {base_currency}_{c}")

    # Compute cross rates
    for i in currencies:
        for j in currencies:
            if i != j and pd.isna(M.loc[i, j]):
                if not pd.isna(M.loc[i, base_currency]) and not pd.isna(M.loc[j, base_currency]):
                    M.loc[i, j] = M.loc[i, base_currency] - M.loc[j, base_currency]
                else:
                    missing_rates += 1
                    logger.warning(f"Cannot compute cross rate for {i}_{j}")
    
    # Check matrix quality
    if missing_rates > 0:
        logger.warning(f"Matrix has {missing_rates} missing rates")
        
    # Check for arbitrage opportunities
    check_arbitrage(M)
    
    return M

def check_arbitrage(M, threshold=1e-8):
    """
    Check for triangular arbitrage opportunities in the log rate matrix.
    
    In an arbitrage-free market, for any currencies i,j,k:
    log(R_ij) + log(R_jk) + log(R_ki) = 0
    
    Parameters:
    -----------
    M : pandas.DataFrame
        The log exchange rate matrix
    threshold : float
        Threshold for considering a deviation as an arbitrage opportunity
        
    Returns:
    --------
    list
        List of detected arbitrage opportunities
    """
    currencies = M.index.tolist()
    opportunities = []
    
    for i in currencies:
        for j in currencies:
            if i == j:
                continue
                
            for k in currencies:
                if i == k or j == k:
                    continue
                    
                # Check if we have all required rates
                if pd.isna(M.loc[i, j]) or pd.isna(M.loc[j, k]) or pd.isna(M.loc[k, i]):
                    continue
                
                # Calculate triangular sum (should be zero in arbitrage-free market)
                arb = M.loc[i, j] + M.loc[j, k] + M.loc[k, i]
                
                if abs(arb) > threshold:
                    opportunity = {
                        'path': f"{i} -> {j} -> {k} -> {i}",
                        'deviation': arb,
                        'profit_percent': (np.exp(arb) - 1) * 100
                    }
                    opportunities.append(opportunity)
                    logger.warning(
                        f"Arbitrage opportunity: {opportunity['path']}, "
                        f"Deviation: {arb:.8f}, "
                        f"Profit: {opportunity['profit_percent']:.4f}%"
                    )
    
    return opportunities