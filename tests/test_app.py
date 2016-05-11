
from mock import patch

from record_recommender.app import RecordRecommender, get_config, setup_logging


@patch('os.path.exists', return_value=False)
@patch('os.environ.get', return_value=None)
def test_get_config(mock_os_exisits, mock_os_env):
    """Test admin views."""
    config = get_config(config_path=None)
    assert config == {}
