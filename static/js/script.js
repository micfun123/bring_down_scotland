function refreshData() {
    fetch('/api/refresh', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        }
    })
    .then(response => response.json())
    .then(data => {
        if (data.status === 'success') {
            location.reload();
        } else {
            alert('Error refreshing data');
        }
    })
    .catch(error => {
        console.error('Error:', error);
        alert('Error refreshing data');
    });
}

// Auto-refresh every 5 minutes
setTimeout(() => {
    console.log('Auto-refreshing data...');
    refreshData();
}, 300000);