import "./globals.css";

export const metadata = {
  title: "ArbiView — 跨所资金费率套利",
  description: "Binance 与 Bybit 永续合约资金费率套利机会"
};

export default function RootLayout({ children }) {
  return (
    <html lang="zh-CN">
      <body>{children}</body>
    </html>
  );
}
