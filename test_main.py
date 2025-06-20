# tests/test_student2.py
import pytest
import io
import csv
import tempfile
from main import (
    ValidationError,
    parse_csv,
    validate_row,
    calculate_total_revenue,
    revenue_by_modele,
    revenue_by_ville,
    average_duration_per_modele,
    top_n,
    top_models_by_city,
    monthly_revenue,
    average_rotation,
    predict_trend
)

def create_csv(content):
    """Helper: write CSV content to a temp file and return its path."""
    tmp = tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.csv', newline='')
    tmp.write(content)
    tmp.flush()
    return tmp.name

# Sample CSV with valid and invalid rows
CSV_CONTENT = """
transaction_id,date,ville,type,modele,prix,duree_location_mois
TX1,2023-01-10,Paris,vente,Audi A4,1000.0,
TX2,2023-02-15,Marseille,location,Peugeot 208,500.0,10
TX3,2023-03-20,Lyon,location,Renault Clio,300.0,5
TX4,2023-04-01,Paris,vente,BMW X3,1500.0,
INVALID,,Paris,vente,Audi A4,-100.0,
"""

@pytest.fixture(scope='module')
def sample_csv(tmp_path_factory):
    path = create_csv(CSV_CONTENT)
    return path

def test_validate_row_good():
    row = {'transaction_id': 'TX1', 'date': '2023-01-10', 'ville': 'Paris',
           'type': 'vente', 'modele': 'Audi A4', 'prix': '1000.0', 'duree_location_mois': ''}
    validated = validate_row(row.copy())
    assert validated['transaction_id'] == 'TX1'
    assert isinstance(validated['prix'], float) and validated['prix'] == 1000.0
    assert validated['duree_location_mois'] is None

@pytest.mark.parametrize('bad_row, error_msg', [
    ({'transaction_id': '', 'date': '2023-01-10', 'ville': 'Paris', 'type': 'vente', 'modele': 'A', 'prix': '100', 'duree_location_mois': ''}, "Missing 'transaction_id'"),
    ({'transaction_id': 'TX', 'date': '', 'ville': 'Paris', 'type': 'vente', 'modele': 'A', 'prix': '100', 'duree_location_mois': ''}, "Missing 'date'"),
    ({'transaction_id': 'TX', 'date': '2023-01-10', 'ville': '', 'type': 'vente', 'modele': 'A', 'prix': '100', 'duree_location_mois': ''}, "Missing 'ville'"),
    ({'transaction_id': 'TX', 'date': '2023-01-10', 'ville': 'Paris', 'type': 'invalid', 'modele': 'A', 'prix': '100', 'duree_location_mois': ''}, "Invalid 'type'"),
    ({'transaction_id': 'TX', 'date': '2023-01-10', 'ville': 'Paris', 'type': 'vente', 'modele': '', 'prix': '100', 'duree_location_mois': ''}, "Missing 'modele'"),
    ({'transaction_id': 'TX', 'date': '2023-01-10', 'ville': 'Paris', 'type': 'vente', 'modele': 'A', 'prix': '-1', 'duree_location_mois': ''}, "'prix' must be non-negative"),
    ({'transaction_id': 'TX', 'date': '2023-01-10', 'ville': 'Paris', 'type': 'location', 'modele': 'A', 'prix': '100', 'duree_location_mois': ''}, "Missing 'duree_location_mois'"),
])
def test_validate_row_errors(bad_row, error_msg):
    with pytest.raises(ValidationError) as excinfo:
        validate_row(bad_row)
    assert error_msg in str(excinfo.value)

def test_parse_csv_counts(sample_csv):
    rows = list(parse_csv(sample_csv))
    # One invalid row should be skipped
    assert len(rows) == 4

def test_calculation_functions(sample_csv):
    rows = list(parse_csv(sample_csv))
    # Total revenue
    total = calculate_total_revenue(rows)
    assert total == pytest.approx(1000.0 + 500.0 + 300.0 + 1500.0)
    # Revenue by modèle
    rev_model = revenue_by_modele(rows)
    assert rev_model['Audi A4'] == pytest.approx(1000.0)
    assert rev_model['Peugeot 208'] == pytest.approx(500.0)
    # Revenue by city
    rev_city = revenue_by_ville(rows)
    assert rev_city['Paris'] == pytest.approx(1000.0 + 1500.0)
    assert rev_city['Marseille'] == pytest.approx(500.0)
    # Avg duration
    avg_dur = average_duration_per_modele(rows)
    assert avg_dur['Peugeot 208'] == pytest.approx(10.0)
    # Top n
    top = top_n({'A': 10, 'B': 5, 'C': 8}, n=2)
    assert top == [('A', 10), ('C', 8)]

def test_time_series_and_trend(sample_csv):
    rows = list(parse_csv(sample_csv))
    m_rev = monthly_revenue(rows)
    # Check keys format
    assert '2023-01' in m_rev and '2023-02' in m_rev
    # Predict trend returns next month and numeric value
    next_month, pred = predict_trend(rows)
    assert isinstance(next_month, str) and '-' in next_month
    assert isinstance(pred, float)

def test_top_models_by_city(sample_csv):
    rows = list(parse_csv(sample_csv))
    tops = top_models_by_city(rows, n=1)
    # Each city has at least one model
    assert all(len(v)==1 for v in tops.values())

def test_average_rotation(sample_csv):
    rows = list(parse_csv(sample_csv))
    avg_rot = average_rotation(rows)
    # Only two locations: durations 10 and 5
    assert avg_rot == pytest.approx((10+5)/2)
