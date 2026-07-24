/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  output: "standalone",
  async rewrites() {
    return [{
      source: "/backend/:path*",
      destination: `${process.env.BACKEND_URL || "http://127.0.0.1:8080"}/api/:path*`
    }];
  }
};

export default nextConfig;
