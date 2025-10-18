# Set the project name and executable for this folder
add_executable(example main.cc)

# Optionally, link any libraries or dependencies here, if required
target_link_libraries(example PRIVATE db RapidJSON)