import { parseFids, parseLoopbackAddr } from "./server";

describe("dashboard input parsing", () => {
  test("only loopback listen addresses are accepted", () => {
    expect(parseLoopbackAddr("127.0.0.1:8766")).toEqual({ host: "127.0.0.1", port: 8766 });
    expect(parseLoopbackAddr("localhost:9000")).toEqual({ host: "localhost", port: 9000 });
    expect(parseLoopbackAddr("[::1]:8766")).toEqual({ host: "::1", port: 8766 });
    expect(() => parseLoopbackAddr("0.0.0.0:8766")).toThrow("只允许监听本机地址");
    expect(() => parseLoopbackAddr("192.168.1.5:8766")).toThrow("只允许监听本机地址");
    expect(() => parseLoopbackAddr("127.0.0.1:0")).toThrow("无效端口");
  });

  test("fids are positive integers, deduplicated and sorted", () => {
    expect(parseFids(undefined)).toEqual([]);
    expect(parseFids(["3", 2, "1", 3])).toEqual([1, 2, 3]);
    expect(parseFids("5, 4 4")).toEqual([4, 5]);
    expect(() => parseFids(["1", "-2"])).toThrow("FID 必须是正整数");
    expect(() => parseFids(["1.5"])).toThrow("FID 必须是正整数");
  });
});
