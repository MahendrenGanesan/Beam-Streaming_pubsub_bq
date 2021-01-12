package org.bnp.trades.model;

import java.util.List;

/*
Trades:[
	 {
		 "symbol": "tata",
		 "qty": "-10",
		 "cpty": "c1pf1",
		 "price": 8.95
	 },
	 {
		 "symbol": "tata1",
		 "qty": "-10",
		 "cpty": "c1pf1",
		 "price": 8.95
	 }
]
*/
public class EQTrades {
	private List<Trade> trades;

	public List<Trade> getTrades() {
		return trades;
	}

	public void setTrades(List<Trade> trades) {
		this.trades = trades;
	}

	public class Trade {
		private String cpty;
		private int qty;
		private float price;
		private String symbol;

		public String getSymbol() {
			return symbol;
		}

		public void setSymbol(String symbol) {
			this.symbol = symbol;
		}

		public int getQty() {
			return qty;
		}

		public void setQty(int qty) {
			this.qty = qty;
		}

		public float getPrice() {
			return price;
		}

		public void setPrice(float price) {
			this.price = price;
		}

		public String getCpty() {
			return cpty;
		}

		public void setCpty(String cpty) {
			this.cpty = cpty;
		}

	}
}