import subprocess

def test_main_help():
    result = subprocess.run(
        ["uv", "run", "registry_downloader", "--help"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    assert result.returncode == 0
    assert "Registry Downloader" in result.stdout 