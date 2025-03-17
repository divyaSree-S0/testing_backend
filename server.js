const dotenv = require('dotenv');
const Papa = require('papaparse'); 
const express = require('express');
const cors = require('cors');
const WebSocket = require('ws');
const sqlite3 = require('sqlite3').verbose();
const moment = require('moment');
const momentTz = require('moment-timezone');
const fs = require('fs');
const path = require('path');
const XLSX = require('xlsx');
const app = express();
const { EMA } = require('technicalindicators');

const port = process.env.PORT || 3005;
dotenv.config();

app.use(express.json());
app.use(cors());


const fetchScripMaster = async () => {
    try {
        const response = await fetch("https://Openapi.5paisa.com/VendorsAPI/Service1.svc/ScripMaster/segment/nse_fo");
        const textData = await response.text(); // Get response as text
        
        // Split text into rows
        const rows = textData.trim().split("\n");

        // Define keys based on observed structure (modify if necessary)
        const keys = [
            "Exch", "ExchType", "scrip_code", "Name", "expiry_date", "option_type", "strike_rate",
            "FullName", "TickSize", "LotSize", "QtyLimit", "Multiplier",
            "symbol", "BOCOAllowed", "ISIN", "ScripData", "Series"
        ];
        
        																
        // Convert rows into list of objects
        let dataList = rows.map(row => {
            const values = row.split(",");
            let obj = {};
            keys.forEach((key, index) => {
                obj[key] = values[index] || null; // Handle missing values
            });
            return obj;
        });
        dataList = dataList.filter(row => row.symbol === "NIFTY" && row.option_type !== "XX")
        dataList = dataList.map((row,index) => {
            return {
                symbol : row.symbol,
                scrip_code : row.scrip_code,
                expiry_date : row.expiry_date,
                option_type : row.option_type,
                strike_rate : row.strike_rate,
                ticker : `${row.symbol} ${row.strike_rate} ${row.option_type}`
            }
        })

        return dataList // List of objects
    } catch (error) {
        console.error("Error fetching details:", error);
    }
};

const filePathScripMaster = 'ScripMaster (1).csv';
const csvDataScripMaster = fs.readFileSync(filePathScripMaster, 'utf8');
const dataScripMaster = Papa.parse(csvDataScripMaster, {header: true,}).data
const todayDate = momentTz.tz("Asia/Kolkata").startOf('day').format('YYYY-MM-DD');
const parseDate = (dateString) => {
    const [day, month, year] = dateString.split('-');
    return new Date(`${year}-${month}-${day}`);
};

let globalDate = dataScripMaster.map((row) => {
        return {...row, expiry_date : parseDate(row.expiry_date)}
    }
)

globalDate = globalDate.filter((row) => row.expiry_date >= new Date(todayDate))
globalDate.sort((a, b) => a.expiry_date - b.expiry_date);
const earliestExpiryDate = globalDate[0].expiry_date;
globalDate = earliestExpiryDate.toISOString().split("T")[0].split("-");
globalDate = `${globalDate[2]}-${globalDate[1]}-${globalDate[0]}`

// const dataScripMaster = fetchScripMaster()



const filePath = '3_MIN_2025-03-07.csv';
const csvData = fs.readFileSync(filePath, 'utf8');
let data = Papa.parse(csvData, {header: true,}).data
data = data.map(row => {
    return {
    time : row.time,
    open : parseFloat(row.open),
    high : parseFloat(row.high),
    low : parseFloat(row.low),
    close : parseFloat(row.close)
    // row.highEma = parseFloat(row.high_ema)
    // row.lowEma = parseFloat(row.low_ema)
    }
});
data = data.slice(0,data.length-1)


let wsUrl = "wss://openfeed.5paisa.com/feeds/api/chat?Value1=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1bmlxdWVfbmFtZSI6IjUyODQzOTg2Iiwicm9sZSI6IjI0Mzg5IiwiU3RhdGUiOiIiLCJSZWRpcmVjdFNlcnZlciI6IkEiLCJuYmYiOjE3NDIxODY5NTYsImV4cCI6MTc0MjIzNjE5OSwiaWF0IjoxNzQyMTg2OTU2fQ.-VazbBB1mBOVQHV5dR8lzZo4wNLVqJ_22JBguos7x-E|52843986"
const scripCodes = [999920000];


const candleData = {}
const activeTrades = {};
const liveSharePrice = {};
let subscribedScripCodes = new Set();

const startTime = momentTz().tz('Asia/Kolkata').set({ hour: 9, minute: 22, second: 0, millisecond: 0 })
const endTime = momentTz().tz('Asia/Kolkata').set({ hour: 15, minute: 0, second: 0, millisecond: 0 })
const expiryTime = momentTz().tz('Asia/Kolkata').set({ hour: 15, minute: 15, second: 0, millisecond: 0 })

const updateLiveSharePrice = (scripCode, lastRate, ChgPcnt) => {
    // Check if the scripCode exists; if not, initialize it
    if (!liveSharePrice[scripCode]) {
        liveSharePrice[scripCode] = {
            live: null,
            ChgPcnt: null,
        };
    }
    
    // Update the live price and change percentage
    liveSharePrice[scripCode].live = lastRate;
    liveSharePrice[scripCode].ChgPcnt = ChgPcnt;
};

const updateLiveProfitLoss = (scripCode, currentPrice) => {
    if (activeTrades[scripCode]) {
        activeTrades[scripCode].forEach(trade => {
            const { premium,quantity } = trade; 
            // Calculate live profit or loss
            trade.liveProfitOrLoss = (currentPrice - premium) * quantity; 
        });
    }
};

const subscribeToMarketFeed = () => {
    scripCodes.forEach(scripCode => {
        if (!subscribedScripCodes.has(scripCode)) {
            let subscriptionRequest;
            if (scripCode === 999920000) {
                subscriptionRequest = {
                    Method: "MarketFeedV3",
                    Operation: "Subscribe",
                    ClientCode: "52843986",
                    MarketFeedData: [{ Exch: "N", ExchType: "C", ScripCode: scripCode }]
                };
            } else {
                subscriptionRequest = {
                    Method: "MarketFeedV3",
                    Operation: "Subscribe",
                    ClientCode: "52843986",
                    MarketFeedData: [{ Exch: "N", ExchType: "D", ScripCode: scripCode }]
                };
            }

            ws.send(JSON.stringify(subscriptionRequest));
            console.log(`Subscription request sent for scrip code ${scripCode}`);
            subscribedScripCodes.add(scripCode);  // Mark as subscribed
            activeTrades[scripCode] = [];
        }
    });
};

const extractTimeFromTickDt = (TickDt) => {
    const timestamp = parseInt(TickDt.replace('/Date(', '').replace(')/', ''), 10);
    const date = new Date(timestamp);
    const options = {
        timeZone: 'Asia/Kolkata',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false,
    };
    const parts = date.toLocaleString('en-IN', options).split(', ');
    return `${parts[0]} ${parts[1]}`;
};

const connectWebSocket = () => {
    ws = new WebSocket(wsUrl);

    ws.on('open', () => {
        console.log("WebSocket connection opened");
        subscribeToMarketFeed();
    });

    ws.on('message', (data) => {
        try {
            const response = JSON.parse(data);
            // Ensure response is an array
            if (Array.isArray(response)) {
                response.forEach(item => {
                    // Destructure necessary fields from each item in the array
                    const { LastRate, TickDt, Token, ChgPcnt } = item;
                    if (LastRate && TickDt && Token) {
                        const scripCode = Token; // Use Token as scrip code
                        const formattedTime = extractTimeFromTickDt(TickDt);
                        if ((scripCode === 999920000) && (momentTz(formattedTime, 'DD/MM/YYYY HH:mm:ss').tz('Asia/Kolkata', true).isAfter(startTime))) {
                            updateCandles(scripCode, LastRate, formattedTime);
                        }
                        updateLiveSharePrice(scripCode, LastRate, ChgPcnt);
                        monitorTrades(scripCode, LastRate,formattedTime);
                        updateLiveProfitLoss(scripCode, LastRate);
                    }
                });
            } else {
                console.log("Unexpected response format:", response);
            }
        } catch (err) {
            console.log("Error while parsing WebSocket JSON data", err);
        }
    });

    ws.on('error', (err) => {
        console.log('WebSocket server error. Reconnecting...');
        setTimeout(connectWebSocket, 25000);
    });

    ws.on('close', () => {
        console.log('Disconnected from WebSocket server. Reconnecting...');
        setTimeout(connectWebSocket, 25000);
    });
};

const updateCandles = (scripCode, LastRate, time) => {
    const currentTime = moment(time, 'DD/MM/YYYY HH:mm:ss');
    const currentMinute = currentTime.minutes();
    
    if (!candleData[scripCode]) {
        candleData[scripCode] = { df: JSON.parse(JSON.stringify(data)), currentCandle: null };
    }
    const candles = candleData[scripCode];

    if (!candles.currentCandle) {
        candles.currentCandle = { time: currentTime, open: LastRate, high: LastRate, low: LastRate, close: LastRate };
    } else {
        const previousMinute = moment(candles.currentCandle.time).minutes();

        if (currentMinute === previousMinute) {
            candles.currentCandle.high = Math.max(candles.currentCandle.high, LastRate);
            candles.currentCandle.low = Math.min(candles.currentCandle.low, LastRate);
            candles.currentCandle.close = LastRate;
        } else {
            // const allLowValues = candles.df.slice(-37).map(row => parseFloat(row.low));
            // allLowValues.push(candles.currentCandle.low);
            // const emaLowValues = EMA.calculate({ period : 38, values: allLowValues });
            // candles.currentCandle.lowEma = parseFloat((emaLowValues[emaLowValues.length - 1]).toFixed(2));

            // const allHighValues = candles.df.slice(-37).map(row => parseFloat(row.high));
            // allHighValues.push(candles.currentCandle.high);
            // const emaHighValues = EMA.calculate({ period : 38, values: allHighValues });
            // candles.currentCandle.highEma = parseFloat((emaHighValues[emaHighValues.length - 1]).toFixed(2));
            candles.df.push(candles.currentCandle);
            checkStrategy(scripCode, LastRate, time);
            candles.currentCandle = { time: currentTime, open: LastRate, high: LastRate, low: LastRate, close: LastRate };
        }
    }
    candleData[scripCode] = candles;
};

let red_lots = 0
let red_diff = 0
let red = false
let redBuy = true
let redlotsCount = 0

let green_lots = 0
let green_diff = 0
let green = false
let greenBuy = true
let greenlotsCount = 0

const checkStrategy = (scripCode, LastRate, time) => {
    const candles = candleData[scripCode];
    const checkData = candles.df.slice(-1);

    if (checkData[0].close > checkData[0].open) {
        red = false
        red_lots = 0
        red_diff = 0
        redBuy = true
        redlotsCount = 0
        green_diff += Math.abs(checkData[0].close - checkData[0].open)
        if (green_diff > 20){
            if (green === false){
                green_lots = 1 + Math.floor((green_diff - 20) / 5)
                greenlotsCount += green_lots
                green = true
            }
            else{
                green_lots = Math.floor(Math.abs(checkData[0].close - checkData[0].open) / 5)
                greenlotsCount += green_lots
            }
            if (green_lots > 0 && greenBuy && 
                momentTz(time, 'DD/MM/YYYY HH:mm:ss').tz('Asia/Kolkata', true).isAfter(startTime) && 
                momentTz(time, 'DD/MM/YYYY HH:mm:ss').tz('Asia/Kolkata', true).isBefore(endTime)) {
                console.log(`C B Signal at ${candles.currentCandle.time}, Price: ${candles.currentCandle.close}`);
                executeTrade(LastRate, time, 'C B', 'STR', green_lots);
            }
            greenBuy = greenlotsCount <= 5 ? true : false
        }
    }

    else{
        green = false
        green_lots = 0
        green_diff = 0
        greenBuy = true
        greenlotsCount = 0
        red_diff += Math.abs(checkData[0].close - checkData[0].open)
        if (red_diff > 20){
            if (red === false){
                red_lots = 1 + Math.floor((red_diff - 20) / 5)
                red = true
                redlotsCount += red_lots
            }
            else{
                red_lots = Math.floor((Math.abs(checkData[0].close - checkData[0].open)) / 5)
                redlotsCount += red_lots
            }
            if (red_lots > 0 && redBuy &&
                momentTz(time, 'DD/MM/YYYY HH:mm:ss').tz('Asia/Kolkata', true).isAfter(startTime) && 
                momentTz(time, 'DD/MM/YYYY HH:mm:ss').tz('Asia/Kolkata', true).isBefore(endTime)) {
                console.log(`P B Signal at ${candles.currentCandle.time}, Price: ${candles.currentCandle.close}`);
                executeTrade(LastRate, time, 'P B', 'STR', red_lots);
            }
            redBuy = redlotsCount <= 5 ? true : false
        }
    }
};

const executeTrade = async (buyPrice, time, type, reason, lots) => {
    try {
        let strikePrice, scripCode, premium, target, stopLoss, ticker;
        const buyTime = time;
        const quantity = lots <= 5 ? 75 * lots : 75 * 5;

        const result = await getScripCodeAndStrikePrice(buyPrice, type);
        if (!result) {
            console.error("Failed to get scrip code and strike price");
            return;
        }

        ({ strikePrice, scripCode } = result);

        premium = liveSharePrice[scripCode]?.live || await getPremium(scripCode);
        if (premium === null) {
            console.error("Failed to get premium for scripCode:", scripCode);
            return;
        }

        if (type === "C B") {
            target = buyPrice + 80;
            stopLoss = buyPrice - 20;
            ticker = `NIFTY ${strikePrice} CE`;
        } else {
            target = buyPrice - 80;
            stopLoss = buyPrice + 20;
            ticker = `NIFTY ${strikePrice} PE`;
        }

        if (!activeTrades[scripCode]) {
            activeTrades[scripCode] = [];
        }

        const tradeId = await new Promise((resolve, reject) => {
            db.run(`INSERT INTO buy_trades (ticker, scrip_code, type, price, quantity, stop_loss, target, premium, time, reason, status) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                [ticker, scripCode, type, buyPrice, quantity, stopLoss, target, premium, buyTime, reason, 'active'],
                function (err) {
                    if (err) {
                        console.error("Error inserting trade:", err);
                        reject(err);
                    } else {
                        console.log("Trade inserted successfully with ID", this.lastID);
                        resolve(this.lastID);
                    }
                }
            );
        });

        activeTrades[scripCode].push({
            id: tradeId,
            premium,
            type,
            price: buyPrice,
            quantity,
            stop_loss: stopLoss,
            target,
            liveProfitOrLoss: 0,
            tsl: false,
            selling: false,
        });

        await updateFundsAndProfits(type, premium, null, quantity);

        return tradeId;
    } catch (error) {
        console.error("Error in executeTrade:", error);
        return null;
    }
};

const getScripCodeAndStrikePrice = async (buyPrice, type) => {
    try {
        let strikePrice, scripCode;
        
        if (type === "C B") {
            // Find the nearest lower multiple of 50 for Call options
            strikePrice = Math.floor(buyPrice / 50) * 50;
        } else if (type === "P B") {
            // Find the nearest higher multiple of 50 for Put options
            strikePrice = Math.ceil(buyPrice / 50) * 50;
        } else {
            console.error("Invalid trade type:", type);
            return null;
        }
        
        const optionType = type === "C B" ? 'CE' : 'PE';
        
        const filteredData = dataScripMaster.filter(row => 
            row.symbol === 'NIFTY' && 
            row.expiry_date === globalDate && 
            row.option_type === optionType &&  
            parseFloat(row.strike_rate) === strikePrice
        );
        
        if (filteredData.length > 0) {
            scripCode = filteredData[0].scrip_code;
            
            // Only add to scripCodes if it's not already there
            if (!scripCodes.includes(scripCode)) {
                scripCodes.push(scripCode);
            }
            
            // Subscribe to market feed if websocket is open
            if (ws.readyState === WebSocket.OPEN) {
                await subscribeToMarketFeed();
            }
            
            return { strikePrice, scripCode };
        } else {
            console.log(`No matching scrip code found for strike price: ${strikePrice}`);
            return null;
        }
    } catch (error) {
        console.error("Error in getScripCodeAndStrikePrice:", error);
        return null;
    }
};

const getPremium = async (scripCode) => {
    try {
        const accessToken = process.env.ACCESS_TOKEN;
        const appKey = process.env.USER_KEY; 
      
        const requestBody = {
          head: {
            key: appKey,
          },
          body: {
            MarketFeedData: [
              {
                Exch: 'N',
                ExchType: 'D',
                ScripCode: scripCode,
                ScripData: '',
              },
            ],
            LastRequestTime: '/Date(0)/',
          },
        };
      
        const apiUrl = 'https://Openapi.5paisa.com/VendorsAPI/Service1.svc/V1/MarketFeed';
      
        const response = await fetch(apiUrl, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'Authorization': `bearer ${accessToken}`,
            },
            body: JSON.stringify(requestBody),
        });
      
        if (!response.ok) {
            throw new Error(`Error: ${response.statusText}`);
        }
      
        const data = await response.json();
      
        if (data.head.status === '0') {
            const marketData = data.body.Data;
      
            if (marketData && marketData.length > 0) {
                console.log('Request was successful', marketData[0].LastRate);
                return marketData[0].LastRate;
            } else {
                throw new Error('No market data returned');
            }
        } else {
            throw new Error(`Request failed with status: ${data.statusDescription}`);
        }
    } catch (error) {
        console.error('Error during API request:', error);
        return null;
    }
};

const monitorTrades = async (scripCode, currentPrice, time) => {
    try {
        if (scripCode === 999920000) {
            const currentTime = momentTz(time, 'DD/MM/YYYY HH:mm:ss').tz('Asia/Kolkata', true);

            for (const key of Object.keys(activeTrades)) {
                const trades = activeTrades[key] || [];

                for (const trade of trades) {
                    const { id, stop_loss, target, type, tsl, selling } = trade;
                    let shouldSell = false;
                    let sellReason = '';

                    if (selling) continue;

                    if (type === 'C B') {
                        if (currentPrice > target) {
                            trade.target = Math.max(currentPrice, target);
                            trade.stop_loss = currentPrice - 15;
                            trade.tsl = true;
                        }

                        if (currentPrice <= stop_loss) {
                            sellReason = tsl ? 'tsl triggered' : 'stop_loss';
                            shouldSell = true;
                        } else if (currentTime.isAfter(expiryTime)) {
                            sellReason = 'expired';
                            shouldSell = true;
                        }
                    } else if (type === 'P B') {
                        if (currentPrice < target) {
                            trade.target = Math.min(currentPrice, target);
                            trade.stop_loss = currentPrice + 15;
                            trade.tsl = true;
                        }

                        if (currentPrice >= stop_loss) {
                            sellReason = tsl ? 'tsl triggered' : 'stop_loss';
                            shouldSell = true;
                        } else if (currentTime.isAfter(expiryTime)) {
                            sellReason = 'expired';
                            shouldSell = true;
                        }
                    }

                    if (shouldSell) {
                        console.log(`${sellReason} for trade ${id} at price ${currentPrice}`);
                        trade.selling = true;
                        await sellStock(key, id, sellReason, currentTime, type, currentPrice);
                    }
                }
            }
        }
    } catch (error) {
        console.error("Error in monitorTrades:", error);
    }
};

const updateFundsAndProfits = async (type, buyedPrice, selled_price, quantity) => {
    try {
        // Get current funds data
        const row = await new Promise((resolve, reject) => {
            db.get("SELECT * FROM funds", (err, row) => {
                if (err) {
                    console.error("Error fetching funds data", err);
                    reject(err);
                } else if (row) {
                    resolve(row);
                } else {
                    reject(new Error("No funds data found"));
                }
            });
        });
        
        let totalFunds = row.total_funds;
        let totalProfitLoss = row.total_profit_loss;
        
        if (type === "C B" || type === "P B") {
            totalFunds -= buyedPrice * quantity;
        } else if (type === "C S" || type === "P S") {
            const profit = (selled_price - buyedPrice) * quantity;
            totalFunds += selled_price * quantity;
            totalProfitLoss += profit;
        }
        
        // Update funds in database
        await new Promise((resolve, reject) => {
            db.run("UPDATE funds SET total_funds = ?, total_profit_loss = ?", [totalFunds, totalProfitLoss], (err) => {
                if (err) {
                    console.error("Error updating funds", err);
                    reject(err);
                } else {
                    console.log(`Funds updated. Total Funds: ${totalFunds}, Total Profit/Loss: ${totalProfitLoss}`);
                    resolve();
                }
            });
        });
        
        return { totalFunds, totalProfitLoss };
    } catch (error) {
        console.error("Error in updateFundsAndProfits:", error);
        return null;
    }
};

const sellStock = async (scripCode, tradeId, reason, currentTime, buy_type, currentPrice) => {
    try {
        const tradeIndex = activeTrades[scripCode]?.findIndex(trade => trade.id === tradeId);

        if (tradeIndex === -1) {
            console.log(`Trade ${tradeId} already processed or not found.`);
            return;
        }

        const trade = activeTrades[scripCode][tradeIndex];

        if (!trade.selling) {
            console.log(`Trade ${tradeId} not in selling state, skipping.`);
            return;
        }

        const row = await new Promise((resolve, reject) => {
            db.get(`SELECT * FROM buy_trades WHERE id = ?`, [tradeId], (err, row) => {
                if (err) {
                    console.log("Error fetching buy trade:", err);
                    reject(err);
                } else if (row) {
                    resolve(row);
                } else {
                    reject(new Error(`Trade ${tradeId} not found.`));
                }
            });
        });

        const buyedPrice = row.premium;
        const ticker = row.ticker;
        const quantity = row.quantity;
        const lastTradedPrice = liveSharePrice[scripCode]?.live || await getPremium(scripCode);
        const profitOrLoss = (lastTradedPrice - buyedPrice) * quantity;
        const sell_type = (buy_type === "C B") ? 'C S' : 'P S';
        const sellTime = currentTime.format('DD/MM/YYYY HH:mm:ss');

        await new Promise((resolve, reject) => {
            db.run(`UPDATE buy_trades SET status = 'closed' WHERE id = ?`, [tradeId], (err) => {
                if (err) {
                    console.log("Error updating trade status:", err);
                    reject(err);
                } else {
                    console.log(`Trade ${tradeId} closed with status: ${reason}`);
                    resolve();
                }
            });
        });

        await new Promise((resolve, reject) => {
            db.run(`INSERT INTO sell_trades (buy_id, ticker, scrip_code, type, price, quantity, premium, time, reason, total_profit) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                [tradeId, ticker, scripCode, sell_type, currentPrice, quantity, lastTradedPrice, sellTime, reason, profitOrLoss],
                function (err) {
                    if (err) {
                        console.log(`Trade ${tradeId} status is closed, but got error in updating db:`, err);
                        reject(err);
                    } else {
                        console.log("Sell executed successfully");
                        resolve();
                    }
                }
            );
        });

        activeTrades[scripCode].splice(tradeIndex, 1);
        console.log(`Removed trade ${tradeId} from activeTrades for scripCode: ${scripCode}`);
        if (activeTrades[scripCode].length === 0) {
            delete activeTrades[scripCode]; // Remove the empty array
            delete liveSharePrice[scripCode]
            const index = scripCodes.indexOf(scripCode);
            if (index > -1) {
                scripCodes.splice(index, 1);
            }
            subscribedScripCodes.delete(scripCode);
            console.log(`Removed scripCode ${scripCode} from scripCodes and subscribedScripCodes.`);
            if (ws.readyState === WebSocket.OPEN) {
                const unsubscribeRequest = {
                    Method: "MarketFeedV3",
                    Operation: "Unsubscribe",
                    ClientCode: "52843986",
                    MarketFeedData: [{ Exch: "N", ExchType: "D", ScripCode: scripCode }] //or C if it is 999920000
                };
                ws.send(JSON.stringify(unsubscribeRequest));
                console.log(`Unsubscription request sent for scrip code ${scripCode}`);
            }
        }

        await updateFundsAndProfits(sell_type, buyedPrice, lastTradedPrice, quantity);

        return profitOrLoss;
    } catch (error) {
        console.error(`Error in sellStock for trade ${tradeId}:`, error);
        return null;
    }
};
  
let db = new sqlite3.Database(`./trading.db-${momentTz().tz('Asia/Kolkata').format('DD-MM-YYYY')}`, (err) => {
    if (err) {
        console.error("Error opening database", err);
    } else {
        console.log("Database connection successful");
        createTables();
    }
});

const createTables = () => {
    db.run(`CREATE TABLE IF NOT EXISTS buy_trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT,
        scrip_code INTEGER,
        type VARCHAR(10),
        price REAL,
        quantity INTEGER,
        stop_loss REAL,
        target REAL,
        premium INTEGER,
        time TEXT,
        status TEXT,
        reason TEXT,
        initial TEXT
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS sell_trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        buy_id INTEGER,  -- This is the foreign key referring to the buy_trades table
        ticker TEXT,
        scrip_code INTEGER,
        type VARCHAR(10),
        price REAL,
        quantity INTEGER,
        premium REAL,  -- Changed to REAL to match buy_trades premium
        time TEXT,
        reason TEXT,
        total_profit REAL,
        FOREIGN KEY (buy_id) REFERENCES buy_trades(id)  -- Correct foreign key syntax
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS funds (
        total_funds REAL,
        total_profit_loss REAL
    )`, (err) => {
        if (err) {
            console.log("Funds table creation error", err);
        } else {
            db.get("SELECT COUNT(*) as count FROM funds", (err, row) => {
                if (row.count === 0) {
                    db.run("INSERT INTO funds (total_funds, total_profit_loss) VALUES (650000, 0)");
                }
            });
        }
    });

    db.run(`CREATE TABLE IF NOT EXISTS tokens (
        access_token text
    )`, (err) => {
        if (err) {
            console.log("tokens table creation error", err);
        }
    });

};

app.post('/login', async (req, res) => {
    const { totp } = req.body;

    // Validate TOTP parameter
    if (!totp) {
        return res.status(400).json({ error: 'TOTP is required!' });
    }

    try {
        // Step 1: Get Request Token
        const requestToken = await getRequestToken(totp);
        
        // Step 2: Get Access Token using the Request Token
        const accessToken = await getAccessToken(requestToken);
        wsUrl = `wss://openfeed.5paisa.com/feeds/api/chat?Value1=${accessToken}|52843986`;
        console.log(wsUrl)
        const now = momentTz.tz("Asia/Kolkata");
        const nextTime = now.clone().add(1, 'minute').startOf('minute');  // Move to the next full minute
        const delay = nextTime.diff(now);
        setTimeout(() => {
            connectWebSocket();
        }, delay);


        // If both tokens are successfully obtained
        res.status(200).json({
            success: 'Login successful',
            accessToken: accessToken  // Optionally return accessToken for client usage
        });
    } catch (error) {
        res.status(error.status || 400).json({ error: error.message || 'An error occurred during login.' });
    }
});

async function getRequestToken(totp) {
    const url = 'https://Openapi.5paisa.com/VendorsAPI/Service1.svc/TOTPLogin';
    const requestBody = {
        "head": {
            "Key": process.env.USER_KEY
        },
        "body": {
            "Email_ID": process.env.EMAIL_ID,
            "TOTP": totp,
            "PIN": process.env.PIN
        }
    };

    try {
        const response = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(requestBody)
        });
        console.log(response)

        if (!response.ok) {
            throw new Error('Error while logging in.');
        }

        const data = await response.json();

        if (data.body && data.body.Status === 0) {
            return data.body.RequestToken;  // Return RequestToken if successful
        } else {
            throw new Error(data.body ? data.body.Message : 'Unknown error while requesting token.');
        }
    } catch (error) {
        throw { status: 400, message: error.message };
    }
}

async function getAccessToken(requestToken) {
    const url = 'https://Openapi.5paisa.com/VendorsAPI/Service1.svc/GetAccessToken';
    const requestBody = {
        "head": {
            "Key": process.env.USER_KEY
        },
        "body": {
            "RequestToken": requestToken,
            "EncryKey": process.env.ENCRYPTION_KEY,
            "UserId": process.env.USER_ID
        }
    };

    try {
        const response = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(requestBody)
        });

        if (!response.ok) {
            throw new Error('Error while fetching access token.');
        }

        const data = await response.json();

        if (data.body && data.body.Status === 0) {
            const accessToken = data.body.AccessToken;
            await new Promise((resolve, reject) => {
                db.run(`INSERT INTO tokens (access_token) VALUES (?)`, [accessToken], function (err) {
                    if (err) {
                        reject(new Error('Error inserting access token into the database.'));
                    } else {
                        resolve();
                    }
                });
            });
            return accessToken;
        } else {
            throw new Error(data.body ? data.body.StatusDescription : 'Unknown error while fetching access token.');
        }
    } catch (error) {
        throw { status: 400, message: error.message };
    }
}

app.listen(port, () => {
    console.log(`Server running on port ${port}`);
});

const now = momentTz.tz("Asia/Kolkata");
const nextTime = now.clone().add(1, 'minute').startOf('minute');  // Move to the next full minute
const delay = nextTime.diff(now);
setTimeout(() => {
    connectWebSocket();
}, delay);



const currentTimeToSave = momentTz().tz('Asia/Kolkata');
const targetTimeToSave = momentTz.tz('15:31:00', 'HH:mm:ss', 'Asia/Kolkata');
if (currentTimeToSave.isAfter(targetTimeToSave)) {
    targetTimeToSave.add(1, 'days');
}
const timeDifference = targetTimeToSave.diff(currentTimeToSave);
console.log('Time difference:', timeDifference);
const today_date = momentTz().tz('Asia/Kolkata').format('YYYY-MM-DD');

setTimeout(() => {
    const csvString = Papa.unparse(candleData[999920000].df);
    const outputFilePath = `3_MIN_${today_date}.csv`;
    fs.writeFileSync(outputFilePath, csvString, 'utf8');
    console.log(`CSV file has been saved as ${outputFilePath}.csv`);
    process.exit()
}, timeDifference);


let maxProfit = 0; // Initialize to negative infinity
let minProfit = 0;  // Initialize to positive infinity

app.get("/details", async (req, res) => {
    try {
        const [buyTrades, sellTrades, funds, activeTradesData, liveSharePriceData, tradingSummary] = await Promise.all([
            getBuyTrades(),
            getSellTrades(),
            getFunds(),
            getActiveTrades(),
            getLiveSharePrice(),
            getTradingSummary()
        ]);

        // const buyCount = {};
        // buyTrades.forEach(trade => {
        //     if (trade.status === 'active') {
        //         if (buyCount[trade.scripCode]) {
        //             buyCount[trade.scripCode]++;
        //         } else {
        //             buyCount[trade.scripCode] = 1;
        //         }
        //     }
        // });

        const totalProfitLoss = calculateTotalProfitLoss(activeTradesData);
        const totalProfit = funds.total_profit_loss + totalProfitLoss;

        maxProfit = Math.max(maxProfit, totalProfit);
        minProfit = Math.min(minProfit, totalProfit);

        res.status(200).json({
            buyTrades,
            sellTrades,
            funds,
            activeTrades: activeTradesData,
            liveSharePrice: liveSharePriceData,
            totalProfitLoss,
            maxProfit,
            minProfit,
            tradingSummary,
            // buyCount,
            dataScripMaster,
            globalDate
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

const getActiveTrades = () => {
    return new Promise((resolve) => {
        resolve(activeTrades);
    });
};

const getLiveSharePrice = () => {
    return new Promise((resolve) => {
        resolve(liveSharePrice);
    });
};

const calculateTotalProfitLoss = (activeTradesData) => {
    let totalProfitLoss = 0;

    if (activeTradesData) {
        Object.entries(activeTradesData).forEach(([scripCode, trades]) => {
            trades.forEach(trade => {
                totalProfitLoss += trade.liveProfitOrLoss;
            });
        });
    }

    return totalProfitLoss;
};

const getBuyTrades = () => {
    return new Promise((resolve, reject) => {
        db.all("SELECT * FROM buy_trades", [], (err, rows) => {
            if (err) {
                reject(err);
            } else {
                resolve(rows);
            }
        });
    });
};

const getSellTrades = () => {
    return new Promise((resolve, reject) => {
        db.all("SELECT * FROM sell_trades", [], (err, rows) => {
            if (err) {
                reject(err);
            } else {
                resolve(rows);
            }
        });
    });
};

const getFunds = () => {
    return new Promise((resolve, reject) => {
        db.get("SELECT * FROM funds", [], (err, row) => {
            if (err) {
                reject(err);
            } else {
                resolve(row);
            }
        });
    });
};


const getTradingSummary = async () => {
    try {
        // Get all completed trades (sell trades)
        const sellTrades = await getSellTrades();
        
        // Calculate summary statistics
        const totalTrades = sellTrades.length;
        let winningTrades = 0;
        let losingTrades = 0;        
        let totalProfit = 0;
        let totalLoss = 0;
        
        sellTrades.forEach(trade => {
            if (trade.total_profit > 0) {
                winningTrades++;
                totalProfit += trade.total_profit;
            } else if (trade.total_profit < 0) {
                losingTrades++;
                totalLoss += Math.abs(trade.total_profit);
            }
        });
        
        // Calculate win rate and other metrics
        const winRate = totalTrades > 0 ? (winningTrades / totalTrades) * 100 : 0;
        const avgWin = winningTrades > 0 ? totalProfit / winningTrades : 0;
        const avgLoss = losingTrades > 0 ? totalLoss / losingTrades : 0;        
        return {
            totalTrades,
            winningTrades,
            losingTrades,
            winRate,
            avgWin,
            avgLoss,
        };
    } catch (error) {
        console.error("Error calculating trading summary:", error);
        return null;
    }
};