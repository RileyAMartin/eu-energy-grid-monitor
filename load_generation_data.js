    const TYPE_MAPPING = {
        GREEN: [
            "Biomass", "Geothermal", "Hydro Pumped Storage", 
            "Hydro Run-of-river and poundage", "Hydro Water Reservoir", 
            "Marine", "Nuclear", "Other renewable", 
            "Solar", "Wind Offshore", "Wind Onshore"
        ],
        FOSSIL: [
            "Fossil Brown coal/Lignite", "Fossil Coal-derived gas", 
            "Fossil Gas", "Fossil Hard coal", "Fossil Oil", 
            "Fossil Oil shale", "Fossil Peat", "Waste"
        ]
    };

    const COLOR_MAPPING = {
        "Solar": "#F2CC0C",
        "Wind Onshore": "#73BF69",
        "Wind Offshore": "#269966",
        "Hydro Run-of-river and poundage": "#5794F2",
        "Hydro Water Reservoir": "#3274D9",
        "Hydro Pumped Storage": "#1F60C4",
        "Marine": "#3274D9",
        "Biomass": "#96D98D",
        "Geothermal": "#F2495C",
        "Other renewable": "#B7DBAB",
        "Nuclear": "#E02F44",
        "Fossil Gas": "#FF780A",
        "Fossil Coal-derived gas": "#E55C00",
        "Fossil Hard coal": "#333333",
        "Fossil Brown coal/Lignite": "#664433",
        "Fossil Oil": "#4d4141ff",
        "Fossil Oil shale": "#4C3426",
        "Fossil Peat": "#855D42",
        "Waste": "#A352CC",
        "Energy storage": "#00E5FF",
        "Other": "#8E8E8E"
    };

    const getColor = (name) => {
        return COLOR_MAPPING[name] || '#8E8E8E';
    };

    let charts = {};
    let currentRawData = null;


    document.addEventListener('DOMContentLoaded', async () => {
        initCharts();
        await loadMetadata();
        
        window.addEventListener('resize', () => {
            Object.values(charts).forEach(chart => chart.resize());
        });
    });

    function initCharts() {
        const theme = 'dark'; 
        const initOpts = { renderer: 'canvas', backgroundColor: 'transparent' };
        
        echarts.registerTheme('grafana', {
            backgroundColor: 'transparent',
            textStyle: { fontFamily: 'Inter, sans-serif' }
        });

        charts.stack = echarts.init(document.getElementById('viz_generation_stack'), 'grafana', initOpts);
        charts.pie = echarts.init(document.getElementById('viz_pie'), 'grafana', initOpts);
        charts.aggregated = echarts.init(document.getElementById('viz_aggregated'), 'grafana', initOpts);
        charts.gauge = echarts.init(document.getElementById('viz_gauge'), 'grafana', initOpts);
        
        bindHighlightAction(charts.stack);
        bindHighlightAction(charts.pie);
        bindHighlightAction(charts.aggregated);

        Object.values(charts).forEach(c => c.showLoading({ color: '#5794f2', textColor: '#ccccdc', maskColor: 'rgba(24, 27, 31, 0.8)' }));
    }

    async function loadMetadata() {
        try {
            const response = await fetch('data/generation/metadata.json');
            const metadata = await response.json();
            
            const selector = document.getElementById('zoneSelector');
            metadata.sort((a, b) => a.label.localeCompare(b.label));

            metadata.forEach(item => {
                const option = document.createElement('option');
                option.value = item.file;
                option.textContent = item.label;
                selector.appendChild(option);
            });

            if (metadata.length > 0) {
                selector.value = metadata[0].file;
                loadZoneData(metadata[0].file);
            }

        } catch (error) {
            console.error("Failed to load metadata:", error);
            alert("Error loading dashboard configuration.");
        }
    }

    // --- Data Processing & Rendering ---

    async function loadZoneData(filename) {
        Object.values(charts).forEach(c => c.showLoading());
        
        try {
            const response = await fetch(`data/generation/${filename}`);
            currentRawData = await response.json();
            
            renderCurrentData();
            
        } catch (error) {
            console.error(`Failed to load data for ${filename}:`, error);
        } finally {
            Object.values(charts).forEach(c => c.hideLoading());
        }
    }

    function renderCurrentData() {
        if (!currentRawData) return;

        const hoursBack = parseInt(document.getElementById('timeSelector').value);

        let maxTime = 0;
        Object.values(currentRawData).forEach(series => {
            series.forEach(point => {
                const t = new Date(point[0]).getTime();
                if (t > maxTime) maxTime = t;
            });
        });

        const cutoffTime = maxTime - (hoursBack * 60 * 60 * 1000);

        const timestampSet = new Set();
        
        Object.values(currentRawData).forEach(series => {
            series.forEach(point => {
                const t = new Date(point[0]).getTime();
                if (t >= cutoffTime) {
                    timestampSet.add(point[0]);
                }
            });
        });

        const sortedTimestamps = Array.from(timestampSet).sort();

        const seriesMW = {};
        const seriesMWh_Total = {}; 
        const aggregatedMW = { 
            "Clean Fuels": new Array(sortedTimestamps.length).fill(0),
            "Fossil Fuels": new Array(sortedTimestamps.length).fill(0),
            "Other": new Array(sortedTimestamps.length).fill(0)
        };
        
        let totalGreenMWh = 0;
        let totalMWh = 0;

        for (const [psrType, points] of Object.entries(currentRawData)) {
            const timeMap = new Map(points.map(p => [p[0], { mw: p[1], mwh: p[2] }]));
            
            const alignedMW = [];
            let psrTotalMWh = 0;

            sortedTimestamps.forEach((ts, index) => {
                const val = timeMap.get(ts) || { mw: 0, mwh: 0 };
                alignedMW.push(val.mw);
                
                psrTotalMWh += val.mwh;

                if (TYPE_MAPPING.GREEN.includes(psrType)) {
                    aggregatedMW["Clean Fuels"][index] += val.mw;
                } else if (TYPE_MAPPING.FOSSIL.includes(psrType)) {
                    aggregatedMW["Fossil Fuels"][index] += val.mw;
                } else {
                    aggregatedMW["Other"][index] += val.mw;
                }
            });

            if (psrTotalMWh > 0 || Math.max(...alignedMW) > 0) {
                seriesMW[psrType] = alignedMW;
                seriesMWh_Total[psrType] = psrTotalMWh;
                totalMWh += psrTotalMWh;

                if (TYPE_MAPPING.GREEN.includes(psrType)) {
                    totalGreenMWh += psrTotalMWh;
                }
            }
        }

        const formatNumber = (num) => {
            return num.toLocaleString(undefined, { 
                minimumFractionDigits: 0, 
                maximumFractionDigits: 2 
            });
        };

        // --- Render Chart 1: Detailed Stacked Area (MW) ---
        const psrTypes = Object.keys(seriesMW);
        const stackOption = {
            tooltip: { 
                trigger: 'item', 
                axisPointer: { type: 'cross', label: { backgroundColor: '#6a7985' } },
                backgroundColor: '#181b1f',
                borderColor: '#2c3235',
                borderWidth: 1,
                textStyle: { color: '#ccccdc' },
                padding: 10,
                formatter: function (params) {
                    return `
                        ${params.name}<br/>
                        ${params.marker} ${params.seriesName}: <b>${formatNumber(params.value)} MW</b>
                    `;
                }
            },
            legend: { 
                data: psrTypes, 
                type: 'scroll',
                bottom: 0, 
                icon: 'roundRect',
                textStyle: { color: '#ccccdc'},
                pageTextStyle: { color: '#ccccdc' },
                pageIconColor: '#ccccdc',
                pageIconInactiveColor: '#555'
            },
            grid: { left: '5%', right: '4%', bottom: '10%', top: '3%', containLabel: true },
            xAxis: {
                type: 'category', 
                boundaryGap: false, 
                data: sortedTimestamps.map(ts => formatTime(ts)),
                axisLabel: { color: '#8e8e8e' },
                axisLine: { show: false },
                axisTick: { show: false },
                splitLine: { 
                    show: true, 
                    lineStyle: { 
                        color: '#2c3235', 
                        type: 'dashed'
                    } 
                },
                axisPointer: {
                    label: {
                        show: false
                    }
                },
            },
            yAxis: { 
                type: 'value',
                axisPointer: {
                    label: {
                        show: false
                    }
                },
                splitLine: { 
                    lineStyle: { 
                        color: '#2c3235',
                        type: 'dashed'
                    } 
                }, 
                axisLabel: {
                        color: '#8e8e8e',
                        formatter: function (value) {
                            if (value >= 1000) {
                                return (value / 1000).toFixed(0) + ' GW'; // e.g. "30 GW"
                            }
                            return value + ' MW';
                        }
                    }
            },
            series: psrTypes.map(type => ({
                name: type,
                type: 'line',
                stack: 'Total',
                areaStyle: {},
                emphasis: { focus: 'series' },
                showSymbol: false,
                itemStyle: { color: getColor(type) },
                lineStyle: { width: 1 },
                data: seriesMW[type]
            }))
        };
        charts.stack.setOption(stackOption, true);

        const aggKeys = ["Clean Fuels", "Fossil Fuels"];
        const aggColors = { "Clean Fuels": "#73BF69", "Fossil Fuels": "#F2495C", "Other": "#8AB8FF" };
        
        const aggOption = {
            tooltip: { 
                trigger: 'item',
                axisPointer: { type: 'cross', label: { backgroundColor: '#6a7985' } },        
                backgroundColor: '#181b1f',
                borderColor: '#2c3235',
                borderWidth: 1,
                textStyle: { color: '#ccccdc' },
                padding: 10,
                formatter: function (params) {
                    return `
                        ${params.name}<br/>
                        ${params.marker} ${params.seriesName}: <b>${formatNumber(params.value)} MW</b>
                    `;
                }
            },
            legend: {
                data: aggKeys,
                bottom: 0,
                icon: 'roundRect',
                textStyle: {
                    color: '#ccccdc'
                },
                pageTextStyle: { color: '#ccccdc' },
                pageIconColor: '#ccccdc',
                pageIconInactiveColor: '#555'
            },
            grid: { left: '3%', right: '4%', bottom: '10%', top: '3%', containLabel: true },
            xAxis: { 
                type: 'category', 
                boundaryGap: false, 
                data: sortedTimestamps.map(ts => formatTime(ts)),
                axisLabel: { color: '#8e8e8e' },
                axisLine: { show: false },
                axisTick: { show: false },
                splitLine: { 
                    show: true, 
                    lineStyle: { 
                        color: '#2c3235', 
                        type: 'dashed'
                    }
                },
                axisPointer: {
                    label: {
                        show: false
                    }
                },
            },
            yAxis: { 
                type: 'value', 
                name: 'MW', 
                nameTextStyle: { color: '#8e8e8e' },
                splitLine: { 
                    lineStyle: { 
                        color: '#2c3235',
                        type: 'dashed'
                    },
                    showMaxLine: false 
                }, 
                axisLabel: {
                    color: '#8e8e8e',
                    formatter: function (value) {
                        if (value >= 1000) {
                            return (value / 1000).toFixed(0) + ' GW';
                        }
                        return value + ' MW';
                    }
                },
                axisLine: { show: false },
                axisPointer: {
                    label: {
                        show: false
                    }
                },
            },
            series: aggKeys.map(key => ({
                name: key,
                type: 'line',
                stack: 'Total',
                areaStyle: {},
                showSymbol: false,
                itemStyle: { color: aggColors[key] },
                lineStyle: { width: 1 },
                data: aggregatedMW[key]
            }))
        };
        charts.aggregated.setOption(aggOption, true);

        let sortedPieData = Object.entries(seriesMWh_Total)
            .map(([name, value]) => ({ 
                name, 
                value,
                itemStyle: { color: getColor(name) } 
            }))
            .filter(item => item.value > 0)
            .sort((a, b) => b.value - a.value);

        const finalPieData = sortedPieData.slice(0, 8);
        const hiddenData = sortedPieData.slice(8);
        const otherSum = hiddenData.reduce((sum, item) => sum + item.value, 0);

        if (otherSum > 0) {
            const existingOther = finalPieData.find(p => p.name === 'Other');
            if (existingOther) {
                existingOther.value += otherSum;
            } else {
                finalPieData.push({
                    name: 'Other',
                    value: otherSum,
                    itemStyle: { color: getColor('Other') }
                });
            }
        }

        const pieOption = {
            tooltip: { 
                trigger: 'item',
                confine: true,
                backgroundColor: '#181b1f',
                borderColor: '#2c3235',
                borderWidth: 1,
                textStyle: { color: '#ccccdc' },
                padding: 10,
                formatter: function(params) {
                    return `${params.name}: <b>${formatNumber(params.value)} MWh</b> (${params.percent}%)`;
                }
            },
            legend: {
                type: 'scroll',
                orient: 'horizontal',
                align: 'left',
                bottom: 0,
                textStyle: { color: '#ccccdc'},
                pageTextStyle: { color: '#ccccdc' },
                pageIconColor: '#ccccdc',
                pageIconInactiveColor: '#555'
            },
            series: [
                {
                    name: 'Energy Source',
                    type: 'pie',
                    radius: '70%', 
                    center: ['50%', '45%'],
                    avoidLabelOverlap: true,
                    itemStyle: {
                        borderRadius: 0,
                        borderColor: '#181b1f', 
                        borderWidth: 1
                    },
                    label: { show: false },
                    emphasis: { 
                        label: { show: false }, 
                    },
                    data: finalPieData
                }
            ]
        };
        charts.pie.setOption(pieOption, true);

        const greenPercentage = totalMWh > 0 ? (totalGreenMWh / totalMWh * 100).toFixed(1) : 0;
        document.getElementById('gaugeTitle').innerText = `Clean Energy Share (${document.getElementById('timeSelector').options[document.getElementById('timeSelector').selectedIndex].text})`;

        const gaugeOption = {
            series: [
                {
                    type: 'gauge',
                    startAngle: 225,
                    endAngle: -45,
                    radius: '100%',
                    pointer: { show: false },
                    progress: { show: false },
                    detail: { show: false },
                    splitLine: { show: false },
                    axisTick: { show: false },
                    axisLabel: { show: false },
                    axisLine: {
                        lineStyle: {
                            width: 5,
                            color: [
                                [0.2, '#F2495C'],
                                [0.6, '#FF9830'],
                                [1.0, '#73BF69']
                            ]
                        }
                    }
                },
                {
                    type: 'gauge',
                    startAngle: 225,
                    endAngle: -45,
                    radius: '93%',
                    pointer: { show: false },
                    progress: {
                        show: true,
                        overlap: false,
                        roundCap: false, 
                        clip: false,
                        itemStyle: {
                            color: greenPercentage < 20 ? '#F2495C' : (greenPercentage < 60 ? '#FF9830' : '#73BF69')
                        }
                    },
                    axisLine: {
                        lineStyle: {
                            width: 25,
                            color: [[1, '#2c3235']]
                        }
                    },
                    splitLine: { show: false },
                    axisTick: { show: false },
                    axisLabel: { show: false },
                    detail: {
                        width: 50,
                        height: 14,
                        fontSize: 40,
                        color: '#fff',
                        formatter: '{value}%',
                        offsetCenter: ['0%', '15%'] 
                    },
                    title: {
                        show: true,
                        offsetCenter: ['0%', '45%'],
                        color: '#6e7681',
                        fontSize: 12,
                        fontFamily: 'Inter',
                        lineHeight: 18
                    },
                    data: [{
                        value: greenPercentage,
                        name: 'Energy Produced by\nClean Sources'
                    }]
                }
            ]
        };
        charts.gauge.setOption(gaugeOption, true);
    }

    function formatTime(isoString) {
        const date = new Date(isoString);
        return `${date.getDate()}/${date.getMonth()+1} ${String(date.getHours()).padStart(2, '0')}:${String(date.getMinutes()).padStart(2, '0')}`;
    }

function bindHighlightAction(chart) {
    chart.on('legendselectchanged', function (params) {
        const selectedName = params.name;
        const currentSelectedMap = params.selected;
        const allNames = Object.keys(currentSelectedMap);
        const visibleCount = allNames.filter(n => currentSelectedMap[n]).length;
        let newSelected = {};

        if (!currentSelectedMap[selectedName]) {
            if (visibleCount === 0) {
                allNames.forEach(n => newSelected[n] = true);
            } else {
                allNames.forEach(n => newSelected[n] = (n === selectedName));
            }
        } else {
            allNames.forEach(n => newSelected[n] = (n === selectedName));
        }

        chart.setOption({ legend: { selected: newSelected } });
    });
}