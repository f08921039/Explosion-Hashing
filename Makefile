# 設定變數
# ==============================================================================

# 目錄設定
SRCDIR := src
TESTDIR := test
LIBDIR := lib
OBJDIR := obj
DEPSDIR := deps

# 執行檔名稱
TARGET := test_app tail_latency update_test delete_test stable_test

# 靜態函式庫名稱
LIBNAME := libexplosion_hashing.a

# 編譯器與編譯器參數
CC := gcc
# -MMD: 產生相依性檔案，但會忽略系統標頭檔
# -MP: 為每個標頭檔建立一個虛擬目標 (phony target)，當刪除標頭檔時可避免編譯錯誤
CFLAGS := -Wall -Wextra -O3 -g -I$(SRCDIR) -MMD -MP -pthread -DDHT_INTEGER
LDFLAGS := -L$(LIBDIR) -lexplosion_hashing
LIBS = -lnuma

# 尋找所有 .c 原始檔
SRC_FILES := $(wildcard $(SRCDIR)/*.c)
TEST_SRC_FILES := $(wildcard $(TESTDIR)/*.c)

# 從原始檔清單產生對應的物件檔清單
SRC_OBJS := $(patsubst $(SRCDIR)/%.c, $(OBJDIR)/%.o, $(SRC_FILES))
TEST_OBJS := $(patsubst $(TESTDIR)/%.c, $(OBJDIR)/%.o, $(TEST_SRC_FILES))

# 從物件檔清單產生對應的相依性檔清單
DEPS_FILES := $(patsubst $(OBJDIR)/%.o, $(DEPSDIR)/%.d, $(SRC_OBJS)) $(patsubst $(OBJDIR)/%.o, $(DEPSDIR)/%.d, $(TEST_OBJS))

# 預設目標
all: $(LIBDIR)/$(LIBNAME) $(TARGET)

## 目標規則
# ==============================================================================

# 建立執行檔
$(TARGET): $(TEST_OBJS) $(LIBDIR)/$(LIBNAME)
	@echo "連結測試程式..."
	$(CC) $(OBJDIR)/$@.o -o $@ $(LDFLAGS) $(LIBS)

# 建立靜態函式庫
$(LIBDIR)/$(LIBNAME): $(SRC_OBJS)
	@echo "建立靜態函式庫..."
	@mkdir -p $(@D) $(LIBDIR)
	ar rcs $@ $(SRC_OBJS)

# 編譯 src 目錄下的 .c 檔到 obj 目錄，同時產生相依性檔到 deps 目錄
$(OBJDIR)/%.o: $(SRCDIR)/%.c
	@mkdir -p $(@D) $(DEPSDIR) # 確保 obj 和 deps 目錄都存在
	$(CC) $(CFLAGS) -MF $(DEPSDIR)/$*.d -c $< -o $@ $(LIBS)

# 編譯 test 目錄下的 .c 檔到 obj 目錄，同時產生相依性檔到 deps 目錄
$(OBJDIR)/%.o: $(TESTDIR)/%.c
	@mkdir -p $(@D) $(DEPSDIR)
	$(CC) $(CFLAGS) -MF $(DEPSDIR)/$*.d -c $< -o $@ $(LIBS)

# 引入所有自動產生的相依性檔案
# 這裡使用 `-include` 是為了讓 make 在第一次編譯時，即使 .d 檔不存在也不會報錯
-include $(DEPS_FILES)

## 清理目標
# ==============================================================================

.PHONY: clean
clean:
	@echo "清理所有編譯產物..."
	rm -rf $(OBJDIR) $(LIBDIR) $(DEPSDIR) $(TARGET)
