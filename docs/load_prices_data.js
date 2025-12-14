const GRADIENT_COLORS = [
    '#56cc47ff',
    '#D9F0A3',
    '#FEE08B',
    '#f31932ff'
];

let charts = {};
let currentRawData = null;  
let overviewData = null;    
let metadataMap = {};

document.addEventListener('DOMContentLoaded', async () => {
    initCharts();
    await Promise.all([loadMetadata(), loadOverview()]);
    renderLatestPrices();
    
    window.addEventListener('resize', () => {
        Object.values(charts).forEach(chart => chart.resize());
    });
});

function initCharts() {
    const initOpts = { renderer: 'canvas', backgroundColor: 'transparent' };
    
    echarts.registerTheme('grafana', {
        backgroundColor: 'transparent',
        textStyle: { fontFamily: 'Inter, sans-serif' }
    });

    charts.scatter = echarts.init(document.getElementById('viz_scatter'), 'grafana', initOpts);
    charts.heatmap = echarts.init(document.getElementById('viz_heatmap'), 'grafana', initOpts);
    charts.series = echarts.init(document.getElementById('viz_series'), 'grafana', initOpts);
    charts.gauge = echarts.init(document.getElementById('viz_gauge'), 'grafana', initOpts);

    Object.values(charts).forEach(c => c.showLoading({ color: '#5794f2', textColor: '#ccccdc', maskColor: 'rgba(24, 27, 31, 0.8)' }));
}

async function loadOverview() {
    try {
        const response = await fetch('data/prices/overview.json');
        overviewData = await response.json();
    } catch (error) {
        console.error("Failed to load overview data:", error);
    }
}

async function loadMetadata() {
    try {
        const response = await fetch('data/prices/metadata.json');
        const metadata = await response.json();
        
        const selector = document.getElementById('zoneSelector');
        
        metadata.forEach(item => {
            const option = document.createElement('option');
            option.value = item.file;
            option.textContent = item.label;
            option.dataset.code = item.code; 
            selector.appendChild(option);

            metadataMap[item.code] = item.label;
        });

        if (metadata.length > 0) {
            selector.value = metadata[0].file;
            loadZoneData(metadata[0].file);
        }

    } catch (error) {
        console.error("Failed to load metadata:", error);
    }
}

async function loadZoneData(filename) {
    charts.scatter.showLoading();
    charts.heatmap.showLoading();
    charts.series.showLoading();
    
    try {
        const response = await fetch(`data/prices/${filename}`);
        const json = await response.json();
        
        currentRawData = json.history;
        renderCurrentData();
        
    } catch (error) {
        console.error(`Failed to load data for ${filename}:`, error);
    } finally {
        charts.scatter.hideLoading();
        charts.heatmap.hideLoading();
        charts.series.hideLoading();
    }
}

function renderLatestPrices() {
    if (!overviewData || !overviewData.latest) {
        charts.gauge.hideLoading();
        return;
    }

    let barData = [];
    Object.entries(overviewData.latest).forEach(([code, data]) => {
        const label = metadataMap[code] || code;
        barData.push({
            name: label,
            value: data.price,
            code: code
        });
    });

    barData.sort((a, b) => a.value - b.value);

    const rowHeight = 28; 
    const totalHeight = Math.max(250, barData.length * rowHeight);
    
    const container = document.getElementById('viz_gauge');
    container.style.height = `${totalHeight}px`;
    charts.gauge.resize();

    const option = {
        tooltip: {
            trigger: 'item',
            backgroundColor: '#181b1f',
            borderColor: '#2c3235',
            textStyle: { color: '#ccccdc' },
            formatter: params => `<b>${params.name}</b><br/>${params.value.toFixed(2)} €/MWh`
        },
        grid: { top: 0, bottom: 0, left: '2%', right: '10%', containLabel: true },
        xAxis: { 
            type: 'value', 
            show: false, 
            min: 0 
        },
        yAxis: {
            type: 'category',
            inverse: true,
            data: barData.map(item => item.name),
            axisLabel: { 
                color: '#ccccdc', 
                fontSize: 12, 
                margin: 10,
                width: 110,
                overflow: 'truncate'
            },
            axisLine: { show: false },
            axisTick: { show: false },
            splitLine: { show: false }
        },
        visualMap: {
            min: 0,
            max: 100, 
            dimension: 0, 
            inRange: { color: GRADIENT_COLORS },
            show: false
        },
        series: [{
            type: 'bar',
            data: barData.map(item => ({
                value: item.value,
                itemStyle: { borderRadius: [0, 3, 3, 0] }
            })),
            label: {
                show: true,
                position: 'right',
                color: '#fff',
                fontSize: 12,
                fontWeight: 'bold',
                formatter: params => `${params.value.toFixed(1)} €`
            },
            barWidth: 16,     
            barCategoryGap: '40%' 
        }]
    };

    charts.gauge.setOption(option);
    charts.gauge.hideLoading();
}

function renderCurrentData() {
    if (!currentRawData) return;

    const hoursBack = parseInt(document.getElementById('timeSelector').value);
    
    let maxTime = 0;
    currentRawData.forEach(pt => {
        const t = new Date(pt[0]).getTime();
        if (t > maxTime) maxTime = t;
    });
    const cutoffTime = maxTime - (hoursBack * 60 * 60 * 1000);
    
    const filteredData = currentRawData.filter(pt => new Date(pt[0]).getTime() >= cutoffTime);

    if (filteredData.length === 0) return;

    const scatterData = filteredData
        .filter(pt => pt[2] !== null) 
        .map(pt => [pt[2], pt[1]]);   

    charts.scatter.setOption({
        tooltip: {
            trigger: 'item',
            backgroundColor: '#181b1f',
            borderColor: '#2c3235',
            textStyle: { color: '#ccccdc' },
            formatter: params => `Clean Gen: <b>${Math.round(params.value[0])} MW</b><br/>Price: <b>${params.value[1].toFixed(2)} €/MWh</b>`
        },
        grid: { left: '8%', right: '8%', bottom: '15%', top: '15%', containLabel: true },
        xAxis: { 
            name: 'Clean Gen (MW)', nameLocation: 'middle', nameGap: 25,
            type: 'value', scale: true,
            splitLine: { lineStyle: { color: '#2c3235', type: 'dashed' } },
            axisLabel: { color: '#8e8e8e' }
        },
        yAxis: { 
            name: 'Price (€/MWh)', 
            type: 'value', scale: true,
            splitLine: { lineStyle: { color: '#2c3235', type: 'dashed' } },
            axisLabel: { color: '#8e8e8e' }
        },
        series: [{
            symbolSize: 8,
            data: scatterData,
            type: 'scatter',
            itemStyle: { color: '#5794F2', opacity: 0.6 }
        }]
    });

    let minPrice = Infinity;
    let maxPrice = -Infinity;
    
    filteredData.forEach(pt => {
        const val = pt[1];
        if (val < minPrice) minPrice = val;
        if (val > maxPrice) maxPrice = val;
    });

    if (minPrice === maxPrice) maxPrice += 1;

    const heatmapTimeLabels = filteredData.map(pt => {
        const d = new Date(pt[0]);
        return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
    });

    const heatmapData = filteredData.map((pt, index) => {
        return [index, 0, pt[1]];
    });

    charts.heatmap.setOption({
        tooltip: {
            position: 'top',
            backgroundColor: '#181b1f',
            borderColor: '#2c3235',
            textStyle: { color: '#ccccdc' },
            formatter: params => {
                const dateStr = formatTime(filteredData[params.dataIndex][0]); 
                return `<b>${dateStr}</b><br/>Price: <b>${params.value[2].toFixed(2)} €</b>`;
            }
        },
        grid: { 
            top: '5%', bottom: '20%', left: '2%', right: '2%', 
            containLabel: false 
        },
        xAxis: {
            type: 'category',
            data: heatmapTimeLabels,
            axisLabel: { 
                color: '#8e8e8e',
                interval: 'auto',
                showMaxLabel: true
            },
            axisTick: { show: false },
            axisLine: { show: false },
            splitArea: { show: false }
        },
        yAxis: {
            type: 'category', data: [''],
            axisLabel: { show: false },
            axisTick: { show: false },
            axisLine: { show: false },
            splitArea: { show: false }
        },
        visualMap: {
            min: minPrice, 
            max: maxPrice, 
            calculable: true,
            show: true,
            orient: 'horizontal',
            left: 'center',
            bottom: '0%',
            itemWidth: 15,
            itemHeight: 200, 
            textStyle: { color: '#8e8e8e' },
            inRange: { color: GRADIENT_COLORS },
            formatter: function (value) {
                return value.toFixed(0) + ' €'; 
            }
        },
        series: [{
            type: 'heatmap',
            data: heatmapData,
            itemStyle: { borderColor: 'transparent', borderWidth: 0 },
            label: { show: false }
        }]
    });

    charts.series.setOption({
        tooltip: {
            trigger: 'axis',
            backgroundColor: '#181b1f',
            borderColor: '#2c3235',
            textStyle: { color: '#ccccdc' },
            axisPointer: { type: 'cross', label: { backgroundColor: '#6a7985' } },
            formatter: params => `${params[0].name}<br/>Price: <b>${params[0].value.toFixed(2)} €/MWh</b>`
        },
        grid: { left: '3%', right: '4%', bottom: '3%', top: '3%', containLabel: true },
        xAxis: {
            type: 'category', boundaryGap: false,
            data: filteredData.map(pt => formatTime(pt[0])),
            axisLabel: { color: '#8e8e8e' },
            splitLine: { show: true, lineStyle: { color: '#2c3235', type: 'dashed' } }
        },
        yAxis: {
            type: 'value', scale: true,
            splitLine: { lineStyle: { color: '#2c3235', type: 'dashed' } },
            axisLabel: { color: '#8e8e8e', formatter: '{value} €' }
        },
        series: [{
            name: 'Price', type: 'line', showSymbol: false,
            data: filteredData.map(pt => pt[1]),
            lineStyle: { width: 2, color: '#FF9830' },
            areaStyle: {
                color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                    { offset: 0, color: 'rgba(255, 152, 48, 0.5)' },
                    { offset: 1, color: 'rgba(255, 152, 48, 0.0)' }
                ])
            }
        }]
    });
}

function formatTime(isoString) {
    const date = new Date(isoString);
    return `${date.getDate()}/${date.getMonth()+1} ${String(date.getHours()).padStart(2, '0')}:${String(date.getMinutes()).padStart(2, '0')}`;
}