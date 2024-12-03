function updateSummaryStats(summary) {
    if (summary) {
        const statsElements = {
            'total_transactions': document.querySelector('.stats-card:nth-child(1) .stats-value'),
            'total_amount': document.querySelector('.stats-card:nth-child(2) .stats-value'),
            'average_transaction': document.querySelector('.stats-card:nth-child(3) .stats-value')
        };

        for (const [key, element] of Object.entries(statsElements)) {
            if (element && summary[key] !== undefined) {
                if (key.includes('amount') || key.includes('average')) {
                    const numSummary = Number(summary[key])
                    element.textContent = `$${numSummary.toFixed(2)}`;
                } else {
                    element.textContent = summary[key];
                }
            }
        }
    }
}

function updateCharts(range) {
    fetch(`/api/dashboard/data?range=${range}`)
        .then(response => {
            if (!response.ok) {
                console.error('Response status:', response.status);
                console.error('Response statusText:', response.statusText);
                return response.text().then(text => {
                    console.error('Response body:', text);
                    throw new Error(`HTTP error! status: ${response.status}, message: ${text}`);
                });
            }
            return response.json();
        })
        .then(data => {
            updateSummaryStats(data.summary);

            Plotly.newPlot('category-chart', data.category_distribution, {
                title: 'Category Distribution',
                margin: { t: 30, b: 40, l: 40, r: 40 },
                autosize: true,
                responsive: true
            });

            Plotly.newPlot('payment-chart', data.payment_methods, {
                title: 'Payment Methods',
                margin: { t: 30, b: 40, l: 40, r: 40 },
                autosize: true,
                responsive: true
            });

            Plotly.newPlot('daily-chart', data.daily_transactions, {
                title: 'Daily Transactions',
                margin: { t: 30, b: 40, l: 40, r: 40 },
                autosize: true,
                responsive: true
            });
        })
        .catch(error => {
            console.error('Error fetching dashboard data:', error);
            alert('Failed to update dashboard. Please try again later.');
        });
}

document.addEventListener('DOMContentLoaded', function() {
    const dateRange = document.getElementById('dateRange');
    if (dateRange) {
        dateRange.addEventListener('change', function() {
            let range;
            switch(this.value) {
                case '7d':
                    range = 'week';
                    break;
                case '30d':
                    range = 'month';
                    break;
                case '90d':
                    range = 'quarter';
                    break;
                case '1y':
                    range = 'year';
                    break;
                default:
                    range = 'week';
            }
            updateCharts(range);
        });
        updateCharts('week');
    }
});

window.addEventListener('resize', function() {
    const charts = ['category-chart', 'payment-chart', 'daily-chart'];
    charts.forEach(chartId => {
        const chartDiv = document.getElementById(chartId);
        if (chartDiv) {
            Plotly.Plots.resize(chartId);
        }
    });
});