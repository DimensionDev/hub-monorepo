import { BUSINESS_TABLES, businessTables } from "./tables";

describe("business table roadmap", () => {
  test("lists each of the 19 reference tables once", () => {
    const names = BUSINESS_TABLES.map((t) => t.table);
    expect(names).toHaveLength(19);
    expect(new Set(names).size).toBe(19);
  });

  test("a table is done only when a registered projection writes it", () => {
    const grouped = businessTables([{ name: "blocks", table: "blocks" }]);
    const blocks = grouped.find((t) => t.table === "blocks");
    expect(blocks).toMatchObject({ group: "done", projection: "blocks" });
    expect(grouped.filter((t) => t.group === "done")).toHaveLength(1);
    expect(grouped.find((t) => t.table === "fids")).toMatchObject({ group: "planned", projection: null });
    expect(businessTables([]).find((t) => t.table === "blocks")?.group).toBe("planned");
  });
});
